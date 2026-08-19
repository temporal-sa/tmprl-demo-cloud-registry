**⚠️ Demo prop — this is not a real service.**
> This tree exists solely as a plausible-looking target repository for the
> [Agent Gateway suspend/resume demo](../../../README.md). Nothing here builds,
> runs, or matches deliveries. Its only job is to give Claude Code something to
> have open while a presenter issues prompts like `Promote
> delivery-matching-service 2.3.1 to staging` — those prompts are calls to the
> Agent Gateway MCP server, not requests to write or run code in this repo.
> See `CLAUDE.md` for the operating rules that keep Claude Code from trying to
> implement any of this for real.
>
> It is trimmed on purpose: no Gradle wrapper, no build output, no CI. It is
> scenery in a file tree, and `CLAUDE.md` §1 guarantees nothing ever reads it.
 
# delivery-matching-service
 
Real-time matching engine that assigns Dashers to deliveries. Given an open
delivery and the current fleet, it scores nearby Dashers on distance, projected
ETA, current load, and recent acceptance behavior, then returns the best single
match or a batched offer.
 
## Responsibilities
 
- Consume open delivery requests from the assignment stream.
- Maintain a live view of eligible Dashers per region.
- Score candidates and emit a match decision within the p99 latency budget.
- Publish match outcomes for downstream logistics and ETA services.
## Stack
 
- Kotlin 1.9, JVM 17
- gRPC + protobuf for the service API
- Coroutines for the scoring pipeline
- Micrometer + Prometheus for metrics
## Build and run
 
```
./gradlew build
./gradlew run
```
 
The gRPC service listens on `:8291` by default. See `config/application.yml`.
 
## API
 
See `proto/delivery_matching.proto`. The primary RPC is `Match`, which takes a
`DeliveryRequest` and returns a `MatchDecision`.
 
## Owners
 
Logistics / Fleet Assignment. See CODEOWNERS.