# Synthetic AWS read proof preparation

This package is an offline-reviewed fixture helper library. Its presence does
not authorize using credentials or executing AWS calls. The finite commands below do not acquire credentials. There is no
profile/config loader, credential-acquisition fallback, policy mutation,
Terraform operation, seed discovery, general executor, or cleanup API.

A later separately reviewed parent/pipe adapter must supply a short-lived opaque
bootstrap `Credentials`, the exact `Config` digest and approval deadline, and a
private `Journal` whose bootstrap UID, directory identity and manifest digest
were pinned independently. The digest fields are trusted review references;
they are not signatures, live attestation, or discovered ownership. `New`
constructs the production-only transport path when `Run` starts. `Run` is
single-use, even after failure. Retain the journal and all known provider IDs
following any unknown response or persistence acknowledgment.

The seed phase has six ordered SDK dispatches: bootstrap identity, exact allow
and deny metadata (both version-free), then three fixed inert writes with
separate deterministic tokens. No payload body can be supplied by the caller.
The stage-specific body hashes, version IDs and labels are in the renderer's
review artifacts; neither body bytes nor temporary session credentials belong
in Terraform state, receipt files or ordinary output.

The proof phase has four bootstrap dispatches (identity, exact allow metadata,
exact deny metadata, one reader-role assumption), followed by seven scoped
dispatches: reader identity, exact allow metadata, allowed V1 read, denial of a
real alternate V2, denial of a real owned deny-secret V1, denial of direct
GenerateDataKey on the owned deny key, and denial of assuming the owned trap
role. Both native version maps are checked before the reader assumption. The
bootstrap credential handle and transport close before scoped client creation.
The positive read must pass before any negative call. Only AccessDenied or
AccessDeniedException counts as denial; missing resources and unavailable
responses are unproven. Stop at the first mismatch, unknown, or unexpected
permission. An unexpected successful response is retained only as a finite
failure code; its material is discarded, and no follow-on privilege is used.

The reader policy's VersionId condition is intentionally not duplicated as a
KMS SecretVersionId condition. The alternate-version result therefore does not
have that second version-specific confounder. The direct KMS and transitive STS
calls test combined effective authority; they do not isolate every IAM or KMS
condition. This lane proves no CodeBuild, PassRole, Temporal, production, or
candidate/canary authority.

Every operation writes an exclusive requested marker before dispatch. A safe
terminal receipt is written/fsynced/read back afterward. A persistence-unknown
result retains the observed provider outcome separately from its missing ACK;
it is not permission to retry. Reopening checks the pinned private root and
manifest, and never creates a missing store. O_EXCL markers serialize
cooperative processes. This is a controlled local one-shot journal, not a
distributed lease, rollback-resistant witness, or protection against deleting
all records/restoring the entire directory. No reset/prune/recovery-replay API
is provided.

SDK clients have fixed regional HTTPS endpoints, TLS, no proxy, no redirect,
HTTP/2 or keepalive reuse, no ambient configuration, NopRetryer and disabled
clock-skew correction. Private operation capabilities admit one signed HTTP
request only. RPCs are bounded to ten seconds, the overall run to five minutes,
approval to fifteen minutes, assumptions to 900 seconds, and starts to at most
one call per second. The temporary reader credential's usable deadline is
clipped to approval. The library does not provide an account cost hard limit.

Default tests use owned local files and synchronous fake SDK/HTTP responses;
they open no socket and run no child process. The generated-SDK test proves a
single STS dispatch through the same private signer/HTTP guard on a synthetic
503 response. These tests are not live IAM/KMS permission or TLS wire evidence.
The live parent credential-export/pipe invocation, exact preflight, saved-plan
review and each bootstrap/enable/proof action remain separate authorization
scopes. Candidate, canary and build fixtures remain disabled in this lane.

## Finite invocation adapter (live execution still separately reviewed)

`cmd/run-aws-readproof` accepts exactly `--invocation`,
`--invocation-sha256`, `--journal-root`, `--journal-pin`, and
`--journal-pin-sha256`. All are explicit safe metadata. The only credential
source is inherited FD3, an anonymous pipe created by the independently
reviewed parent. The parent sets its read end nonblocking before inheritance,
closes the producer after one bounded credential-process JSON document, and
retains no incidental descriptor copies. The adapter requires ModeNamedPipe,
current UID and a working read deadline before any Read; it never opens a FIFO
path or uses a goroutine fallback. It closes the supplied descriptor on every
outcome. No parent credential export/pipe command is authorized by these docs.

Invocation and external journal pin files are owned regular0600/Nlink1 inside
owned0700 parents. Absolute cleaned paths must have no symlink components.
The invocation is at most32KiB, the pin8KiB, with exact independently supplied
SHA256 pins. Both finite wire structs serialize as `json.Marshal(struct)` plus
one newline, with UTC-Z timestamps and no optional omitted fields. Decode,
unknown-field refusal, EOF and exact byte re-encoding reject all nested
case aliases, duplicates, extra fields and noncanonical whitespace. The pin
must be outside the journal; the journal must already match the independently
pinned directory/manifest/bootstrap UID/spec digest and contain only its
identity file. Existing attempt/unknown files refuse a run before credentials.
There is no run-time initialization or replay fallback.

`Invocation` has ordered fields Version, Contract, Account, Region, ProofID,
SourceFreezeSHA256, BinarySHA256, JournalPinSHA256 and Config. Config copies
Phase, Bindings, SourceAdminRoleID, FixtureExpiresAt, DenyFixtureExpiresAt,
ApprovedUntil and ApprovalSHA256; the clock is never wire input. The adapter
uses real time, fixed account/region/proof, the accepted existing configuration
digest, and both fixture expiry bounds before reading the pipe. The pipe read
is at most ten seconds, additionally bounded by approval/context. The overall
phase context is bounded to five minutes before credential reading. Credential
expiry must cover that remaining bounded phase; an invalid stream never starts
the helper. Successful output requires the exact finite phase step sequence,
expected allow/deny outcomes and acknowledged persistence. Original finite
failures retain separate actual outcome/persistence fields. Raw filesystem,
provider or panic text is never diagnostic output.

`cmd/init-aws-readproof-journal` is a separate OFFLINE command accepting only
`--journal-root`, `--pin`, `--bootstrap-uid` and `--configuration-sha256`.
It requires a precreated new empty owned0700 journal directory and an absent
outside pin in an owned0700 parent. It uses exclusive creation, fsync and
readback; unknown outcomes retain all files without an initialization ACK or
automatic adoption/reset. The canonical outside `JournalPin` fields are
Version, Contract and Binding. Its independent pin can then be reviewed before
writing Invocation; this avoids a circular invocation/pin digest dependency.
Initialization grants no authority to read credentials or perform AWS calls.

Source/binary hashes in Invocation are external audit references, not running
code attestation. The parent must independently verify frozen source, exact
private binary/hash, module/build/config/pin/root and actual role/account,
resource birth, policy, expiry and headroom before authorizing one live run.
Expected digests in argv are equality pins, not signatures. This controlled
local lane assumes no concurrent owner edits/restoration. A concurrent losing
process may consume its explicitly supplied credential stream but cannot
perform a second SourceIdentity dispatch because the helper writes its
exclusive durable request first. No at-most-once credential read, distributed
lease, rollback resistance or automatic retry is claimed.

The adapter tests use synthetic documents/credentials, owned files and pipes,
and private synchronous runner seams. They neither spawn these commands nor
invoke AWS SDK clients, network or real authentication. Actual inherited-FD3
process handoff and exact SSO CLI export behavior remain a separate review and
proof boundary. Accepted helper tests separately exercise generated SDK calls
through fake HTTP responses and synthetic finite orchestration.

## Owned process proof (opt-in, pending actual execution)

The reviewed finite mode is
`python3 scripts/run_operator_go_unit_tests.py --aws-permission-invocation-process`.
Never run `awspermission_process_integration` on the host. This mode extends the
existing pinned native Linux compiler and scratch no-network runtime; it builds
exactly the two unmodified commands above from the same immutable public source
snapshot, records their actual static ELF hashes, and stages no cloud fixtures.
The only selected test is `TestOwnedInvocationFD3ProcessBoundary`; its marker
is injected into that scratch driver only, never the compiler, kernel proof,
or real command children. There is no public fake transport or live test mode.

The driver invokes the actual offline initializer and actual run command with
synthetic data. Its seven fixed cases cover a correct inherited pipe reaching
the durable first SourceIdentity request and finite no-network ProviderFailure,
an ordinary Go pipe rejected before read, absent FD3, malformed input, the real
ten-second stalled-producer deadline, private metadata error, and replay/init
no-reset refusal. The maximum is three init and seven run processes, sequential.
Two roots are retained through the top test: attempted A stays permanently
attempted; B is reused only while each pre-dispatch negative independently
confirms it still contains identity.json alone. There is no live replay.

For Go1.27.3, `ExtraFiles` passes each File through `Fd()`. When Go itself set
nonblocking mode (ordinary `os.Pipe`), `Fd()` restores blocking mode. The correct
parent constructs a raw anonymous pipe with BOTH ends close-on-exec, sets the
read end nonblocking BEFORE `os.NewFile`, and passes only that file as FD3.
The driver checks native flags through SyscallConn without accidentally calling
Fd. Its paired positive/negative cases must prove the actual receiver behavior;
source inspection alone is not process ABI evidence. Closing the producer and
preventing writer inheritance are both required for credential JSON EOF.

Each actual child has a fixed empty credential-free environment, private
HOME/TMPDIR, null stdin, a twenty-second safety context and mandatory reap.
Per-stream private output is capped at32KiB during Write; overflow cancels and
kills/reaps the child rather than accumulating CombinedOutput. The host mode
also enforces a hard aggregate1MiB stream bound and150s deadline, discards
stderr and reconstructs only the exact top/seven subcase names and finite
counter/boolean evidence. Unknown output and private exceptions never become
printed diagnostics. All descriptors and owned containers/images are cleaned
by their existing ownership checks.

Until this mode actually passes under the reviewed kernel/credential boundary,
its source and pure runner guards are preparation only. Even a passing run
would prove local FD3/CLI/file behavior and denial of network reach, not AWS
permissions or a real SSO credential-export handoff. The actual host parent
export/refresh/descriptor implementation remains separately reviewed before
any credentials are read. No account, Temporal or infrastructure changes are
authorized by building or documenting this mode.
