# Worker Configuration Patterns

Use these patterns when the important design question is where activity code
runs or how activity dependencies are owned.

## Worker-Specific Task Queues

Use when later activities must run on the same worker host as an earlier
activity, or when work requires a specific host capability.

Shape:

1. First activity runs on a shared task queue.
2. That activity returns a host-specific task queue name or capability queue.
3. Workflow schedules dependent activities to that specific task queue.
4. Schedule-to-start timeout is set deliberately so unavailable host affinity
   does not wait forever unnoticed.

Use for local files, local sessions, special hardware, elevated access, or other
host-affinity needs. Prefer shared task queues when any worker can run the work.

## Activity Dependency Injection

Use when activities need clients, connection pools, credentials, configuration,
or other process-owned resources.

Shape:

1. Worker startup creates dependencies once.
2. Activity implementation is a class, struct, or object that receives
   dependencies.
3. Worker registers the activity instance or bound methods.
4. Workflow references activity types without owning or constructing external
   clients.

This keeps workflow code deterministic and activities testable.
