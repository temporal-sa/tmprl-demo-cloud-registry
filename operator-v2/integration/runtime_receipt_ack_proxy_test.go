//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type receiptACKWitness struct {
	mu sync.Mutex
	pending,committed journal.Write
	created nativeCreationMetadata
	posts,namespaces,gets,dropped,receiptCAS int
	hideReads atomic.Bool
}

func receiptACKEnvelope(t *testing.T,input nativeProcessInput) journal.Envelope {
	t.Helper()
	r,err:=client.Get(local.Server+"/api/v1/namespaces/"+input.Base.Pin.Parent.Ref.Name+"/configmaps/"+input.Base.Pin.Cell.Record.Ref.Name)
	if err!=nil {t.Fatal("ReceiptACKWitnessReadUnavailable")}
	defer r.Body.Close()
	var cell struct {Metadata metav1.ObjectMeta `json:"metadata"`; Data map[string]string `json:"data"`}
	if r.StatusCode!=200 || json.NewDecoder(io.LimitReader(r.Body,1<<20)).Decode(&cell)!=nil || string(cell.Metadata.UID)!=input.Base.Pin.Cell.Record.Ref.ProviderID {t.Fatal("ReceiptACKWitnessCellChanged")}
	e,err:=journal.Decode([]byte(cell.Data[kubejournal.DataKey]))
	if err!=nil || e.Subject!=input.Base.Pin.Subject {t.Fatal("ReceiptACKWitnessJournalInvalid")}
	return e
}

func receiptACKOriginalPOST(t *testing.T,input nativeProcessInput,store *runtimeinputs.Store,raw []byte) (journal.Write,bool) {
	t.Helper()
	e:=receiptACKEnvelope(t,input)
	if e.Admission==nil || e.Admission.State!=journal.AdmissionActive || len(e.Attempts)!=1 || e.Attempts[0].State!=journal.Observed {return journal.Write{},false}
	a:=e.Attempts[0]
	var pending,birth journal.Write
	for _,row:=range e.Writes {
		if row.Preconditions.Kind==string(input.Kind) {pending=row}
		if row.Preconditions.Kind=="Namespace" {birth=row}
	}
	if pending.State!=journal.Pending || pending.ResultUID!="" || pending.ResultVersion!="" || !pending.Input.MatchesKind(string(input.Kind)) || pending.OperationID!=a.OperationID || pending.Preconditions.ProviderOperationID!=a.OperationID || pending.Preconditions.TargetRevision!=a.Revision || birth.State!=journal.Committed || birth.ResultUID!=a.NamespaceUID || pending.Preconditions.ParentUID!=a.NamespaceUID {return pending,false}
	owner:=ownership.Owner{EnvironmentID:input.Base.Pin.Subject.Environment,InstallationID:input.Base.Pin.Subject.Installation,ProjectUID:input.Base.Pin.Subject.ProjectUID,Lifetime:ownership.Disposable,RunID:local.RunID}
	base,err:=ownership.NewContext(ownership.Config{Owner:owner,ResourceRoot:"lab",Scopes:[]ownership.Scope{{Provider:ownership.Kubernetes,ClusterID:local.RunID}}})
	if err!=nil {return pending,false}
	namespace,err:=renderer.RenderNamespace(base,renderer.NamespaceRequest{Revision:a.Revision,OperationID:a.OperationID,Fingerprint:a.DesiredDigest})
	if err!=nil || namespace.Intent.Name!=birth.Preconditions.Name || namespace.Intent.SpecFingerprint!=birth.Preconditions.Fingerprint {return pending,false}
	i:=namespace.Intent
	parent:=ownership.Record{Ref:ownership.ResourceRef{Scope:i.Scope,Purpose:i.Purpose,Name:i.Name,ProviderID:birth.ResultUID},Owner:i.Owner,CreationOperationID:i.OperationID,CreationIdempotencyKey:i.IdempotencyKey,Revision:i.Revision,SpecFingerprint:i.SpecFingerprint,WriterEpoch:birth.WriterEpoch}
	adapter,_:=typedClient(t)
	bound,err:=adapter.BindNamespace(context.Background(),base,parent,a.OperationID)
	if err!=nil {return pending,false}
	p:=pending.Preconditions
	purpose:=map[runtimefault.Kind]string{runtimefault.Deployment:"deployment",runtimefault.Service:"service"}[input.Kind]
	intent,err:=bound.Context().Intent(purpose,p.Name,p.TargetRevision,p.ProviderOperationID,p.Fingerprint)
	if err!=nil {return pending,false}
	target:=runtimeinputs.Target{CellUID:input.Base.Pin.Cell.Record.Ref.ProviderID,Parent:bound.Parent(),AttemptID:a.OperationID,Revision:a.Revision,DesiredDigest:a.DesiredDigest,Name:p.Name,Fingerprint:p.Fingerprint}
	var desired metav1.Object
	if input.Kind==runtimefault.Deployment {
		payload,e:=store.Resolve(context.Background(),pending.Input,target);if e!=nil{return pending,false};desired=payload.ObjectForProvider()
	} else {payload,e:=store.ResolveService(context.Background(),pending.Input,target);if e!=nil{return pending,false};desired=payload.ObjectForProvider()}
	if desired==nil {return pending,false}
	annotations:=desired.GetAnnotations();if annotations==nil {annotations=map[string]string{}}
	for k,v:=range provider.CreationAnnotations(intent.Intent,ownership.Writer{Epoch:pending.WriterEpoch}) {annotations[k]=v}
	annotations[provider.OperationAnnotation]=intent.Intent.OperationID
	annotations[ownership.ParentNamespaceUIDAnnotation]=bound.Parent().UID
	desired.SetAnnotations(annotations)
	desired.SetOwnerReferences([]metav1.OwnerReference{{APIVersion:"v1",Kind:"Namespace",Name:bound.Parent().Name,UID:types.UID(bound.Parent().UID)}})
	var actual,expected any
	encoded,err:=json.Marshal(desired)
	if err!=nil || json.Unmarshal(raw,&actual)!=nil || json.Unmarshal(encoded,&expected)!=nil {return pending,false}
	return pending,reflect.DeepEqual(actual,expected)
}

func receiptACKProxy(t *testing.T,input nativeProcessInput,store *runtimeinputs.Store) (string,[]byte,*receiptACKWitness) {
	t.Helper()
	target,_:=url.Parse(local.Server)
	forward:=httputil.NewSingleHostReverseProxy(target);forward.Transport=client.Transport
	forward.ErrorHandler=func(w http.ResponseWriter,_ *http.Request,_ error){w.WriteHeader(http.StatusBadGateway)}
	witness:=&receiptACKWitness{}
	collection:="/services";prefix:="/api/v1/namespaces/"
	if input.Kind==runtimefault.Deployment {collection="/deployments";prefix="/apis/apps/v1/namespaces/"}
	cellPath:="/api/v1/namespaces/"+input.Base.Pin.Parent.Ref.Name+"/configmaps/"+input.Base.Pin.Cell.Record.Ref.Name
	forward.ModifyResponse=func(r *http.Response)error {
		if r.Request.Method=="POST" && strings.HasSuffix(r.Request.URL.Path,collection) && r.StatusCode==201 {
			raw,err:=io.ReadAll(io.LimitReader(r.Body,1<<20));r.Body.Close()
			if err!=nil {return err}
			metadata,err:=nativeMetadata(input.Kind,raw)
			if err!=nil || metadata.UID=="" || metadata.Version=="" {t.Error("ReceiptACKNativeResponseInvalid");return io.ErrUnexpectedEOF}
			witness.mu.Lock();witness.created=metadata;witness.mu.Unlock()
			r.Body=io.NopCloser(bytes.NewReader(raw));r.ContentLength=int64(len(raw))
		}
		if r.Request.Method=="PUT" && r.Request.URL.Path==cellPath && r.StatusCode==200 {
			witness.mu.Lock();drop:=witness.receiptCAS==1 && witness.dropped==0 && witness.committed.State==journal.Committed;if drop {witness.dropped++};witness.mu.Unlock()
			if drop {
				witness.hideReads.Store(true)
				r.Body.Close();r.StatusCode=http.StatusServiceUnavailable;r.Status="503 Service Unavailable";r.Header=make(http.Header);r.Body=io.NopCloser(strings.NewReader("receipt acknowledgement unavailable"));r.ContentLength=int64(len("receipt acknowledgement unavailable"))
			}
		}
		return nil
	}
	handler:=http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		if r.Method=="GET" && r.URL.Path==cellPath && witness.hideReads.Load() {w.WriteHeader(http.StatusServiceUnavailable);return}
		if r.Method=="POST" && r.URL.Path=="/api/v1/namespaces" {witness.mu.Lock();witness.namespaces++;witness.mu.Unlock()}
		if r.Method=="POST" && strings.HasSuffix(r.URL.Path,collection) {
			raw,err:=io.ReadAll(io.LimitReader(r.Body,1<<20));r.Body.Close()
			pending,valid:=receiptACKOriginalPOST(t,input,store,raw)
			if err!=nil || !valid {t.Error("ReceiptACKOriginalPendingBodyUnverified");w.WriteHeader(http.StatusConflict);return}
			witness.mu.Lock();witness.posts++;witness.pending=pending;witness.mu.Unlock()
			r.Body=io.NopCloser(bytes.NewReader(raw))
		}
		if r.Method=="PUT" && r.URL.Path==cellPath {
			raw,err:=io.ReadAll(io.LimitReader(r.Body,1<<20));r.Body.Close()
			var cell struct {Data map[string]string `json:"data"`}
			if err!=nil || json.Unmarshal(raw,&cell)!=nil {t.Error("ReceiptACKProposedCellInvalid");w.WriteHeader(http.StatusConflict);return}
			e,err:=journal.Decode([]byte(cell.Data[kubejournal.DataKey]))
			if err!=nil {t.Error("ReceiptACKProposedJournalInvalid");w.WriteHeader(http.StatusConflict);return}
			witness.mu.Lock()
			for _,row:=range e.Writes {
				if row.ID!=witness.pending.ID || row.State!=journal.Committed {continue}
				created:=witness.created
				valid:=row.Preconditions==witness.pending.Preconditions && row.Input==witness.pending.Input && row.WriterEpoch==witness.pending.WriterEpoch && row.ResultUID==created.UID && row.ResultVersion==created.Version && row.Preconditions.ParentUID==created.ParentUID && row.Preconditions.Fingerprint==created.Fingerprint && row.Preconditions.ProviderOperationID==created.Operation
				if input.Kind==runtimefault.Service {valid=valid && row.ResultSpecDigest==created.AllocatedDigest && row.ResultSpecContract==created.SpecContract}
				if !valid {witness.mu.Unlock();t.Error("ReceiptACKCanonicalReceiptMismatch");w.WriteHeader(http.StatusConflict);return}
				if witness.committed.ID=="" {witness.receiptCAS++;witness.committed=row}
			}
			witness.mu.Unlock();r.Body=io.NopCloser(bytes.NewReader(raw))
		}
		if r.Method=="GET" {
			witness.mu.Lock();created:=witness.created;witness.mu.Unlock()
			if created.UID!="" && r.URL.Path==prefix+created.Namespace+collection+"/"+created.Name {
				witness.mu.Lock();blocked:=witness.hideReads.Load() || witness.committed.ID=="";witness.gets++;witness.mu.Unlock()
				if blocked {t.Error("ReceiptACKChildGETBeforeRecovery");w.WriteHeader(http.StatusConflict);return}
			}
		}
		forward.ServeHTTP(w,r)
	})
	endpoint,ca:=startRuntimeDispatchProxy(t,handler)
	return endpoint,ca,witness
}
