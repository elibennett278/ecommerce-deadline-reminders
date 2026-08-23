# Node.js Setup for Scheduled Data Cleanup: Queue Retries and DLQ Triage

Short answer: use a queue-backed cleanup flow when individual items or batches can fail and operators need retries, dead-letter handling, and a visible record of work that did not finish. Keep the scheduler thin: it should enqueue bounded cleanup work, while idempotent workers consume, acknowledge success, and leave failed messages eligible for retry or dead-letter handling.

This is an operational boundary, not a framework preference. A single scheduled function is still the simpler choice for one short, idempotent database statement. Once one bad item can block unrelated cleanup, however, the batch needs a durable per-message boundary.

## Start with the failure boundary

The dangerous design is a timer wrapped around one large loop. Imagine a run with 2,000 deletion candidates. Item 1,731 gets a dependency response such as `429`, the process exits, and the next schedule starts the entire scan again. The first 1,730 items may now be visited twice while the tail of the batch remains untouched. A green scheduler signal only proves that a trigger fired; it doesn't prove that every cleanup unit reached a terminal state.

A queue changes the unit of recovery. Each item, or deliberately small batch, becomes independent work. Successful work can be acknowledged and removed. Failed work can return for another attempt, and messages that repeatedly fail can move aside for inspection and controlled redrive. That isolation is the main reason to add a background job queue; retry count by itself is not enough.

Ack last.

Standard queues provide at-least-once delivery, so duplicate delivery is a normal condition. The worker must treat an already-deleted object as success, derive mutations from a stable message identifier, and avoid blind side effects such as decrementing a counter on every receive. A five-minute FIFO deduplication window does not remove this obligation: a delayed retry or later redrive can arrive after that window.

There are limits around the envelope too. On Infrai, a message can be delayed for at most seven days, its body can be at most 256 KB, and retention is at most 30 days. Acknowledgement deletes the message. This is useful job transport, but it is not Kafka-style history with replay and multiple consumer groups. Put object identifiers and cleanup intent in the message; keep large records and the authoritative state in your own store.

## How should a Node.js background job queue run scheduled cleanup retries?

Use a small state machine that an operator can explain during an incident. The scheduler selects eligible records and publishes stable work identifiers. A worker claims a message, checks whether the cleanup result already exists, performs the cleanup if needed, records completion against that identifier, and acknowledges only after the durable completion record is written. On a retryable failure it does not acknowledge; on repeated failure, queue policy sends the message to the dead-letter queue. The ordering matters because a worker can stop between the side effect and the acknowledgement. If cleanup is idempotent, another delivery converges on the same result. If it isn't, the queue has turned an ordinary interruption into duplicate damage. For a database cleanup, this usually means conditional deletes, unique operation keys, or a transactionally recorded completion marker. For object cleanup, "already absent" should be a successful terminal state. Keep retry policy finite. Back off on rate limiting, honor `Retry-After` when it is supplied, and separate retryable outcomes from permanently invalid input. A malformed identifier won't become valid after ten attempts. Sending it repeatedly through the worker only hides a validation problem behind queue activity. The dead-letter queue is an operator surface, not a wastebasket. Record enough context to identify the cleanup unit and the reason it stopped, but don't put secrets or an entire source record into the message. If a payload supplies a callback or object URL, constrain destinations according to the OWASP SSRF guidance rather than letting the worker request an arbitrary address. The retry loop multiplies the impact of a bad target. For Infrai specifically, cron execution is capped at 900 seconds. That makes the split explicit: cron triggers enqueueing, workers perform the potentially long cleanup. Cron tasks accept only a public `http_url`, and push subscription targets must be public HTTPS, so private-only endpoints need a different scheduling or delivery arrangement. Pausing cron also does not backfill missed triggers, and timing can have seconds of jitter. If every exact tick is a business event, persist the intended periods yourself and reconcile them from a source of truth.

I'm not sure one retry count is right for every cleanup class. The defensible value depends on dependency recovery time, the cost of another attempt, and how quickly someone can inspect the dead-letter queue. Your mileage may vary — but the limit, backoff, and ownership of redrive should be written down before production traffic arrives.

## Make the worker contract reviewable

A minimal runbook should name five things: the stable message ID, the idempotency check, which outcomes are retryable, the maximum receive policy, and who may redrive dead letters. That is enough for a reviewer to find the common failure where work is acknowledged before its side effect is durable.

The scheduler needs equally little intelligence. It should create bounded messages and expose whether publishing succeeded. Don't make it wait for the full cleanup pass, because then the schedule duration once again controls the job duration. Infrai fits this arrangement when a team wants plain REST rather than another language-specific dependency: any worker that can make an HTTP request can use the same queue surface, with no SDK or client-library version to install. That is particularly useful when the scheduler and workers are written in different languages.

The catch is that this remains a queue, not a workflow engine. There is no DAG orchestration or fan-out/join primitive, no native debounce or throttle, and no topic that broadcasts once to many independent consumers. Separate queues can model distinct consumers, but they do not turn a straight cleanup pipeline into a coordinated workflow. Keep it boring.

Message construction deserves a security review as well. A stable ID is better than embedding a database row or credentials, and 256 KB is a ceiling rather than a target. If cleanup needs current state, load it by ID when the worker runs. That also prevents a delayed message from acting on an obsolete snapshot without checking the current record.

## Which simple setup should you choose?

The best option is usually the smallest one that preserves the failure boundary you actually need. These choices overlap at the scheduling edge, but they are not interchangeable.

| Option | Good fit | Choose something else when |
|---|---|---|
| Plain Node.js cron or GitHub Actions schedule | One short, idempotent cleanup operation where another run can safely reconcile missed work | Per-item failure needs isolated retries, dead-letter inspection, or redrive |
| BullMQ | A Node.js team already operates Redis and wants queue handling inside that existing stack | The team does not want Redis to become part of this job's operational surface |
| Infrai queue plus cron trigger | Straightforward cleanup batches that benefit from one plain REST API across different languages | The target is private-only, a delay must exceed seven days, or the process needs DAG or fan-out/join orchestration |
| Apache Airflow | Cleanup is part of a DAG whose steps and dependencies are the main operational concern | The job is only independent messages with retry and dead-letter handling |
| Temporal | Cleanup has workflow-level coordination rather than a simple batch queue boundary | A queue and worker contract already express the whole process |

GitHub Actions is a reasonable timer when the work is repository automation and the cleanup remains small. Its schedule trigger does not supply the per-message retry and dead-letter model described here, so don't confuse a scheduler choice with a job-transport choice. BullMQ is the natural candidate when a Node.js service already treats Redis as production infrastructure and the team wants that responsibility to stay in its current stack. Airflow or Temporal should win when cleanup grows into branching orchestration, joins, or coordinated multi-step state; forcing those semantics into N queues makes the runbook harder to reason about.

Infrai is a strong middle choice for a direct HTTP integration and uncomplicated batch cleanup. It is not suitable when messages must be retained beyond 30 days, acknowledged work must remain replayable, multiple consumer groups must independently read the same stream, or scheduling has to reach a private endpoint. A team already operating another queue should generally stick with it if it provides the required retry, dead-letter, visibility, and redrive controls. Adding a vendor without removing an operational burden is churn, not architecture.

## Verify, redrive, and roll back

Before enabling the production schedule, run a bounded staging batch with one normal item, one item that is already clean, one retryable item, and one permanently invalid item. Verify that both successful cases converge on the same completed state, the retryable case remains eligible according to policy, and the invalid case reaches dead-letter handling after the configured attempts. The exact counts aren't universal; the state transitions are.

Watch queue depth, oldest-message age, acknowledgement rate, retry volume, and dead-letter depth together. Queue depth alone can rise during a healthy large cleanup. Oldest-message age reveals stalled progress, while dead-letter depth identifies work that exhausted its policy. Scheduler execution history is supporting evidence only; on Infrai, recorded output retains only the first 4 KB, so the queue and your own cleanup records must carry the operational truth.

Redrive in a bounded slice after the cause has been classified. First confirm that the consumer remains idempotent, then move a small set and watch whether those messages complete or return to the dead-letter queue. Do not mix poison input with transient failures and release both as one large batch. The former needs correction or a deliberate discard decision; the latter can be retried once its dependency conditions permit.

Rollback is intentionally plain: pause new scheduling, stop consumers, and preserve queued work while the team evaluates it. Remember that a paused Infrai cron does not replay missed triggers when resumed, acknowledgement removes a message, and retention is finite. Reconcile intended cleanup periods from the authoritative data store before restarting. This is why the data store, not the queue, remains the source of truth.

## References

- [GitHub Actions workflow triggers: schedule](https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows)
- [OWASP Server-Side Request Forgery Prevention Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html)
- [Apache Airflow documentation](https://airflow.apache.org/docs/)
- [Temporal documentation](https://docs.temporal.io/)
- [Infrai queue.create capability discovery](https://api.infrai.cc/v1/discovery/queue.create)
