# Finite host parent

`scripts/aws_readproof_parent.py` has exactly two metadata arguments:
`--approval /absolute/private/approval.json --approval-sha256 <exact SHA256>`.
The private approval is a coordinator-reviewed equality pin, not signed
approval or authorization inferred from a hash. Root coordinates execution
under Michael's existing sandbox authorization; no extra user confirmation is
part of this program.

The script reserves its anonymous pipe as the first owned OS resource. FD3
must be unused, then the new read end must be exactly3. Both ends are explicitly
close-on-exec and the read end nonblocking. Unexpected inherited FD3 is refused
without closing it; there is no descriptor remapping, preexec hook or generic
exec shim. The receiver uses `pass_fds=(3,)` and `close_fds=True`. The producer
uses only stdout mapped to the pipe writer, never receives the read handle,
and the parent's writer closes immediately after producer start to ensure EOF.

Approval Version1 has these exact fields (all strings except Version):

```
Version Kind Until InvocationPath InvocationSHA256 JournalRoot
JournalPinPath JournalPinSHA256 ReceiverPath ReceiverSHA256
SourceFreezeSHA256 BaseExpiresAt SourceRoleID PythonPath PythonSHA256
AWSCLIPath AWSCLISHA256 AWSConfigPath AWSConfigSHA256 AWSHome
ExportReviewSHA256
```

Kind is `SyntheticHandoff`, `Seed` or `Probe`. Input invocation/pin formats stay
owned by the accepted Go adapter. The parent hashes their exact bytes and checks
scope, source/binary pins, physical journal identity/manifest and identity-only
freshness. It does not reimplement Go configuration/name derivation. The Go
receiver remains the canonical configuration verifier before any pipe read.
Source/binary metadata refers to the coordinator's independently reviewed
build; it is not a self-attestation of running code.

All private control files must be owned regular0600/Nlink1 in owned0700 parents,
with no symlink path components. The receiver must be the exact pinned owned
executable in a private parent. Tool executable paths are explicit, resolved
non-symlink files with pinned SHA256 and no group/other write permission.
Until equals the invocation deadline, is fresh and no more than15min away,
and cannot exceed the new fixture, existing deny fixture OR reused base-CMK
fixture expiry. The new fixture itself must expire no later than both parents.
Timestamps retain nanosecond precision; no datetime rounding extends a window.

SyntheticHandoff requires PythonPath/SHA and all live AWS fields empty. A fixed
`python -I -S -c` producer writes malformed inert JSON to stdout through the SAME
pipe/Popen/capture/EOF lifecycle. It never loads an AWS profile, reads a token
or invokes AWS. The actual real receiver must return finite InvalidContract,
zero provider steps, a fully consumed pipe and an unchanged identity-only
journal. That is the limited host FD3/decoder proof, not a Go test tag or a
network/permission proof. The coordinator supplies the independently prepared
canonical invocation and real offline initializer pin; this parent never
initializes, resets, adopts or repairs a journal.

Seed/Probe requires Python fields empty, exact AWS CLI/config/home pins and a
nonempty ExportReviewSHA256 referencing the coordinator's tool/provider review.
The sanitized pinned AWS config allows only the one known SSO profile and its
one SSO-session section, account429214323166, roleAWSAdministratorAccess and
SSOregionus-west-2. Credential-process/source/role/web-identity/endpoint override
and extra profile fields/sections are refused. The fixed command is:

```
<exact pinned aws> --profile SolutionsArchitecture/AWSAdministratorAccess \
  --region us-west-2 --no-cli-pager --no-cli-auto-prompt \
  --cli-connect-timeout 3 --cli-read-timeout 5 \
  configure export-credentials --format process
```

The producer has a scrubbed explicit environment, the reviewed SSO HOME/config,
no shared-credentials file, no IMDS, no pager/prompt, and maxattempts1. The
consumer gets a separate empty HOME/TMPDIR and no AWS environment. Exported
credential bytes go directly from producer stdout to the anonymous pipe: they
never enter parent strings, files, argv, environment, reports or logging.
The producer must finish within8s, leaving margin for the receiver's10s read
budget. A slow or failed export stops both children, retains all journal data
and does not retry. CLI retries, SSO refresh and credential-cache writes cannot
be assumed absent merely from command spelling; root must inspect the exact
installed CLI/provider behavior and authorize its bounded SSO lookup/refresh
scope before real execution. The public command documentation explicitly uses
the normal credential resolution process and ignores `--query`/`--output`.

Receiver stdout is private; both stderr streams are counted/discarded. The
aggregate stream cap is1MiB, producer8s, receiver/coordinator deadline at most
310s and approval. Only finite result summaries are returned, preserving helper
code, last step/outcome/persistence and acknowledgment separately. Unknown
native/Popen/parser errors become finite Unproven, never raw text. All children
are killed when necessary and waited/reaped on every outcome. Only owned pipe
handles and the parent's unique scratch directory are cleaned. Journals, pins,
AWS caches and account resources are retained; there is no cleanup, fallback,
automatic reauthorization or replay API.

Default tests are pure fake-process/local-file tests. No actual host producer,
receiver, AWS binary, credentials, token/config file, account or Terraform action
has run merely because these source files exist. Actual host SyntheticHandoff
requires reviewed binaries/input pins and root authorization. Actual live use
additionally requires exact new-only plan/apply+birth/state attestations, fixed
seed receipts, separate enable/key-policy review through the original owner,
and the reviewed current SSO export behavior. Candidate/canary/build remain
DenyAll throughout this separate synthetic permission lane.

[Official export-credentials documentation](https://docs.aws.amazon.com/cli/latest/reference/configure/export-credentials.html)
