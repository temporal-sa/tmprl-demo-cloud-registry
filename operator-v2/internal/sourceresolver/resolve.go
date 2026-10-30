package sourceresolver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/buildplan"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
)

type Resolver struct {
	policy TransportPolicy
	auth   PrivateAuthProvider
	store  SourceStore
	limits Limits
	now    func() time.Time
	client func(TransportPolicy, Limits, time.Time, func() time.Time) (*http.Client, func(), error)
}

// New constructs activity capabilities, never registers a global Git transport.
// Production callers must independently authorize policy/store/provider scope.
func New(policy TransportPolicy, auth PrivateAuthProvider, store SourceStore, limits Limits, now func() time.Time) (*Resolver, error) {
	if store == nil || now == nil {
		return nil, invalid("SourceCapabilityRequired", "Source acquisition requires explicit storage, clock and transport capabilities.")
	}
	_, close, err := newHTTPPolicy(policy, limits, time.Time{}, now)
	if err != nil {
		return nil, err
	}
	close()
	policy.AllowedIPs = append([]netip.Addr(nil), policy.AllowedIPs...)
	policy.RootCAs = policy.RootCAs.Clone()
	return &Resolver{policy: policy, auth: auth, store: store, limits: limits, now: now, client: newHTTPPolicy}, nil
}

// Resolve accepts raw authoring and repeats trusted source policy validation.
// SourceSelection and returned DTO fields are not authentication credentials.
func (r *Resolver) Resolve(ctx context.Context, authored project.Document, bindings buildplan.Bindings) (Artifact, error) {
	ctx, cancel := context.WithTimeout(ctx, r.policy.Timeout)
	defer cancel()
	selection, err := buildplan.PrepareSource(authored, bindings)
	if err != nil {
		return Artifact{}, err
	}
	if selection.Owner != r.policy.Owner || selection.Binding.CheckoutURL != r.policy.Repository {
		return Artifact{}, invalid("SourceScopeMismatch", "The approved source differs from this resolver owner and transport capability.")
	}
	if selection.PinnedCommit && len(selection.RequestedRef) != 40 {
		return Artifact{}, invalid("UnsupportedSourceObjectFormat", "This resolver supports full SHA-1 Git commits; SHA-256 repositories require a reviewed adapter.")
	}
	if !selection.PinnedCommit && strings.HasPrefix(selection.RequestedRef, "refs/") && !strings.HasPrefix(selection.RequestedRef, "refs/heads/") {
		return Artifact{}, invalid("UnsupportedSourceRef", "Source resolution supports exact branch refs or explicitly pinned full commits.")
	}
	if _, err = watchDigest(nil, selection); err != nil {
		return Artifact{}, invalid("UnsupportedSourceWatchFilter", "The approved watch filter syntax requires the separate Flux parity capability; no credential or source was accessed.")
	}
	reservation, err := r.store.Reserve(ctx, selection)
	if err != nil {
		return Artifact{}, invalid("SourceRecordUnavailable", "The pinned owned source record is unavailable, replaced or has an unresolved commit.")
	}
	if reservation.SourceUID == "" || reservation.BindingHash != buildplan.SourceBindingHash(selection.Binding) {
		return Artifact{}, invalid("SourceRecordMismatch", "The source record reservation differs from approved source policy.")
	}
	// Auth lookup only after source scope and actual owned record verification.
	var auth transport.AuthMethod
	expires := time.Time{}
	if selection.Binding.AuthPolicy != "None" {
		if r.auth == nil {
			return Artifact{}, failure("SourceCredentialRequired", "Private source acquisition requires an explicit scoped credential provider.", diagnostics.Authentication)
		}
		request := AuthRequest{private: &authRequestData{owner: selection.Owner, repository: selection.Binding.CheckoutURL, logicalName: selection.Binding.AuthLogicalName, versionRef: selection.Binding.AuthVersionRef, bindingHash: buildplan.SourceBindingHash(selection.Binding)}}
		credential, err := r.auth.Credential(ctx, request)
		if err != nil || credential.private == nil || credential.private.request != request.value() || credential.private.username == "" || credential.private.password == "" || !r.now().Before(credential.private.expiresAt) || credential.private.expiresAt.Sub(r.now()) > 15*time.Minute {
			return Artifact{}, failure("SourceCredentialDenied", "The private source credential is unavailable, expired or outside its exact approved scope.", diagnostics.Authentication)
		}
		auth = &githttp.BasicAuth{Username: credential.private.username, Password: credential.private.password}
		expires = credential.private.expiresAt
	}
	if !expires.IsZero() {
		var expireCancel context.CancelFunc
		ctx, expireCancel = context.WithDeadline(ctx, expires)
		defer expireCancel()
	}
	client, close, err := r.client(r.policy, r.limits, expires, r.now)
	if err != nil {
		return Artifact{}, err
	}
	defer close()
	protocol := githttp.NewClientWithOptions(client, &githttp.ClientOptions{RedirectPolicy: githttp.NoFollowRedirects})
	endpoint, err := transport.NewEndpoint(selection.Binding.CheckoutURL)
	if err != nil {
		return Artifact{}, invalid("SourceEndpointInvalid", "The approved source endpoint is not supported.")
	}
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/")
	session, err := protocol.NewUploadPackSession(endpoint, auth)
	if err != nil {
		return Artifact{}, sourceFetchError(err)
	}
	defer session.Close()
	refs, err := session.AdvertisedReferencesContext(ctx)
	if err != nil {
		return Artifact{}, sourceFetchError(err)
	}
	var commit plumbing.Hash
	if selection.PinnedCommit {
		commit = plumbing.NewHash(selection.RequestedRef)
	} else {
		name := selection.RequestedRef
		if !strings.HasPrefix(name, "refs/heads/") {
			if strings.HasPrefix(name, "refs/") {
				return Artifact{}, invalid("UnsupportedSourceRef", "Source resolution supports exact branch refs or explicitly pinned full commits.")
			}
			name = "refs/heads/" + name
		}
		var found bool
		commit, found = refs.References[name]
		if !found || commit.IsZero() {
			return Artifact{}, invalid("SourceRefUnavailable", "The exact approved branch is not advertised by the approved repository.")
		}
	}
	request := packp.NewUploadPackRequest()
	request.Wants = []plumbing.Hash{commit}
	if refs.Capabilities.Supports(capability.Shallow) {
		_ = request.Capabilities.Set(capability.Shallow)
		request.Depth = packp.DepthCommits(1)
	}
	// Deliberately do not request thin packs, sideband/progress, submodules or LFS.
	response, err := session.UploadPack(ctx, request)
	if err != nil {
		return Artifact{}, sourceFetchError(err)
	}
	data, readErr := io.ReadAll(io.LimitReader(response, r.limits.PackBytes+1))
	closeErr := response.Close()
	if readErr != nil || closeErr != nil || int64(len(data)) > r.limits.PackBytes {
		return Artifact{}, invalid("SourcePackLimit", "The approved source pack exceeds its bounded transport contract.")
	}
	objects, err := decodePack(ctx, data, r.limits)
	if err != nil {
		return Artifact{}, invalid("SourcePackInvalid", "The source pack is invalid, unsupported or exceeds pre-inflation object/delta limits.")
	}
	archive, watch, files, err := fullArchive(ctx, objects, commit, selection, r.limits)
	if err != nil {
		if errors.Is(err, errWatch) {
			return Artifact{}, invalid("UnsupportedSourceWatchFilter", "The requested watch filter or repository sourceignore requires the separate Flux parity capability; build inputs were not pruned.")
		}
		return Artifact{}, invalid("SourceTreeUnsupported", "The source tree is invalid or exceeds limits; symlinks, submodules and LFS pointer fallback are unsupported.")
	}
	sum := sha256.Sum256(archive)
	facts := Facts{Owner: selection.Owner, CheckoutURL: selection.Binding.CheckoutURL, RequestedRef: selection.RequestedRef, PolicyRevision: selection.Binding.PolicyRevision, BindingHash: buildplan.SourceBindingHash(selection.Binding), Commit: commit.String(), ArtifactDigest: "sha256:" + hex.EncodeToString(sum[:]), WatchSelectionDigest: watch, ArchiveFormat: ArchiveFormat, WatchProfile: WatchProfile, Files: files, ArchiveBytes: int64(len(archive))}
	receipt, err := r.store.Commit(ctx, reservation, facts, archive)
	if err != nil {
		return Artifact{}, invalid("SourceCommitUnconfirmed", "Source content was acquired but its owned archive/record commit was not acknowledged; retain partial evidence.")
	}
	if receipt.SourceUID != reservation.SourceUID || receipt.Version == 0 || !reflect.DeepEqual(receipt.Facts, facts) {
		return Artifact{}, invalid("SourceReceiptMismatch", "The committed source record does not match independently acquired content facts.")
	}
	source := buildplan.ResolvedSource{Owner: facts.Owner, CheckoutURL: facts.CheckoutURL, RequestedRef: facts.RequestedRef, PolicyRevision: facts.PolicyRevision, BindingHash: facts.BindingHash, Commit: facts.Commit, ArtifactDigest: facts.ArtifactDigest, SourceUID: receipt.SourceUID, SourceVersion: strconv.FormatUint(receipt.Version, 10)}
	return Artifact{private: &artifactData{source: source, summary: Summary{Commit: facts.Commit, ArtifactDigest: facts.ArtifactDigest, WatchSelectionDigest: watch, SourceUID: source.SourceUID, SourceVersion: source.SourceVersion, ArchiveFormat: ArchiveFormat, WatchProfile: WatchProfile, Files: files, ArchiveBytes: facts.ArchiveBytes}, archive: archive}}, nil
}
func sourceFetchError(err error) error {
	if errors.Is(err, errRemoteDenied) {
		return failure("SourceNotAccessible", "The approved repository is not accessible with the scoped source policy.", diagnostics.Authentication)
	}
	return failure("SourceFetchFailed", "The scoped HTTPS Git exchange failed; no alternate transport, repository or credential was attempted.", diagnostics.Transient)
}
