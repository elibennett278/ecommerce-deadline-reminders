# Simplest DLQ Redrive for Failed Background Jobs in a Small Node.js SaaS

Use the queue you already operate when it can retain failed jobs and replay a bounded batch, otherwise reach for a small redrive service with a generic queue adapter. The service should select, rate-limit, audit, and stop; it should not invent another scheduler.

I run cron and queue infrastructure in production, and I've been paged for both missed jobs and duplicate deliveries. The second kind changes how I judge "simple." A redrive button is simple only after the handler is idempotent, the batch is reviewable, and the operator can halt it without taking the worker fleet down.

## How should a small Node.js SaaS redrive failed background jobs from a DLQ?

Start with the narrowest design: one dead-letter queue per workload and region, a stable job identifier, and an operator-triggered redrive that copies selected envelopes back to the normal work queue at a fixed rate. Keep the Node.js worker unchanged. The redrive process is control-plane code, while the worker remains the data plane that validates and executes the job.

Short answer: don't buy or build a separate scheduling system just to retry dead-lettered work. Use an existing queue capability if it exposes the failed messages, preserves the payload and job identifier, supports bounded replay, and gives you enough audit data to answer who replayed what. Build a thin service only when several queues need one approval path or when operators otherwise need production credentials on their laptops.

The smallest safe service has five operations: preview a filtered batch, approve its immutable batch ID, enqueue at a configured ceiling, cancel future sends, and report per-job outcomes. It must not delete the source record as soon as it sends a copy. Retain that record until the destination confirms acceptance and your retention policy permits removal. That makes an interrupted run resumable without guessing where it stopped.

| Approach | Good fit | Operational catch |
| --- | --- | --- |
| Queue-native redrive | One queue technology, one team, infrequent incidents | Approval and cross-region reporting may remain manual |
| Thin internal redrive service | Several workloads need the same preview, rate, and audit rules | Your team owns its authorization and deployment |
| Durable workflow engine | Multi-step work lasts a long time and needs recorded state transitions | It is more machinery than a single retry loop |
| Cron plus a queue | Periodic creation of jobs that execute asynchronously | Cron triggers work; it doesn't replace dead-letter handling |

Cron is useful for periodic triggers, but its job is scheduling commands at specified times. A queue serves a different boundary: it decouples the producer from asynchronous consumers. Keeping those responsibilities separate has made my runbooks much easier to reason about at 03:00.

## Make replay safety a property of the handler

A DLQ doesn't prove that a job produced no side effect. It proves only that the delivery path eventually classified the message as unsuccessful. The worker may have committed a database row or called an external dependency before it lost the chance to record completion. Assume at-least-once delivery at the application boundary, then make a repeated business operation harmless.

I learned this with a duplicate-write bug. One naive retry loop replayed the same operation twice: the first attempt had inserted 26 invoice rows, but its acknowledgement wasn't recorded, so my second pass inserted those 26 rows again. The queue did what I asked. I had used a fresh delivery ID as the deduplication key instead of the stable invoice operation ID, and the database therefore saw two unrelated writes. I stopped the redrive, compared invoice operation IDs rather than queue delivery IDs, and marked the accepted subset before touching the remaining messages. The useful evidence was in the database ledger, not in the worker's attempt counter. Once I could account for every selected operation, I removed the duplicate rows through the normal correction path and changed the handler to claim the business operation under a unique constraint before applying its effect. That was the entire incident, but it permanently changed my redrive checklist: preview the business keys, verify the claim constraint, then send the canary.

Never derive the idempotency key from the retry attempt. Derive it from the business event, such as `account_id + billing_period + operation`, and carry it unchanged through the active queue, DLQ, preview, and redrive. The handler should atomically claim that key with the local state transition whenever both live in the same database. For an external side effect, pass the same key if the dependency accepts one and record enough state to reconcile an ambiguous response.

This is the gate: if you can't explain what the second execution does, don't redrive yet.

The catch is that some work isn't naturally idempotent. Sending a one-time human notification or invoking a dependency without idempotency support may require a ledger and a manual decision rather than automated replay. Stick with per-message review for those jobs. I'm not sure there is a universal abstraction that makes an irreversible side effect safe; your mileage may vary, but a generic `retry()` wrapper certainly doesn't.

## Implement a bounded redrive loop

I prefer a small Go control-plane binary even when the application workers are Node.js. That choice isn't a claim about either runtime. It keeps the emergency tool independent of the application release and gives me one boring executable in the runbook — no package installation during an incident.

The useful abstraction is intentionally dull. A source lists immutable dead-letter envelopes, a destination accepts the original envelope, and an audit sink records each accepted copy. The caller supplies a context so cancellation stops future sends. The limiter below is fixed rather than adaptive because an operator should be able to predict the maximum pressure a run will add.

```go
package redrive

import (
	"context"
	"fmt"
	"time"
)

type Envelope struct {
	JobID   string
	Payload []byte
}

type Source interface {
	List(ctx context.Context, batchID string, limit int) ([]Envelope, error)
}

type Destination interface {
	Enqueue(ctx context.Context, job Envelope) error
}

type Audit interface {
	Accepted(ctx context.Context, batchID, jobID string) error
}

func Run(ctx context.Context, batchID string, limit, perSecond int,
	src Source, dst Destination, audit Audit) error {
	if batchID == "" || limit < 1 || perSecond < 1 {
		return fmt.Errorf("invalid redrive request")
	}

	jobs, err := src.List(ctx, batchID, limit)
	if err != nil {
		return err
	}

	tick := time.NewTicker(time.Second / time.Duration(perSecond))
	defer tick.Stop()

	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}

		if err := dst.Enqueue(ctx, job); err != nil {
			return fmt.Errorf("enqueue %s: %w", job.JobID, err)
		}
		if err := audit.Accepted(ctx, batchID, job.JobID); err != nil {
			return fmt.Errorf("audit %s: %w", job.JobID, err)
		}
	}
	return nil
}
```

Keep selection outside `Run`. A preview should freeze the exact job IDs and payload hashes under `batchID`, then an approval should authorize that frozen set. Filtering a live DLQ while sending creates a moving target. Also cap both batch size and send rate; retries can reproduce the same downstream pressure that caused messages to exhaust their normal attempts.

## Separate US and EU queues before choosing a service

For a small SaaS serving US and EU workloads, I start with two regional work queues, two regional DLQs, and separate credentials. A redrive request names exactly one region. Its preview reads there, its destination writes there, and its audit record stays associated with that region. This isn't a statement about any particular legal regime; it is an architecture rule that prevents the operational tool from becoming an accidental cross-region data mover.

Use the same binary and configuration schema in both regions, but don't use a single unscoped credential. Put the region, queue, filter, count, payload-hash digest, requested rate, requester, approver, and expiration into the batch record. The approval must fail closed if any of those fields change. Two engineers should be able to compare the preview with the completion report without opening job payloads.

The service choice comes after those boundaries are clear. I use this decision test:

| Question | Prefer queue-native control | Prefer a thin shared service |
| --- | --- | --- |
| How many queue types exist? | One | Several behind stable adapters |
| Who performs redrive? | A small on-call group | Several teams with scoped roles |
| How often does it happen? | Rarely | Often enough to standardize review |
| Is regional isolation required? | Existing consoles and credentials enforce it | The service can enforce and audit it centrally |
| Are side effects idempotent? | Required | Still required |

Don't centralize merely to get a nicer screen. A shared service expands the authorization surface and becomes another component to deploy. It is not suitable when a single queue's native operation already meets the runbook, or when jobs require case-by-case business approval. Conversely, a manual script is a poor fit once several teams need consistent regional scoping and immutable approvals. The simplest option is the one with the fewest new failure decisions, not the fewest lines of code.

## Verify the batch and define rollback before sending

Before approval, sample payloads without copying sensitive fields into chat, confirm the handler version is ready, and run the selected IDs through a dry-run validator. Record the DLQ count and age distribution. Then redrive a canary batch at a low fixed rate and watch accepted jobs, handler completions, deduplication skips, latency, and new dead-letter arrivals. Those signals need workload and region labels; a global total hides the queue that is still unhealthy.

Stop quickly.

Cancellation is the first rollback action, but it only prevents future enqueue calls. It cannot pull back work already accepted by the destination, so the runbook must distinguish `selected`, `accepted`, `started`, `completed`, and `deduplicated`. If the canary behaves unexpectedly, cancel the context, preserve the frozen batch, and investigate the accepted subset. Don't delete or mutate the source records to make a dashboard look clean.

After the canary meets the workload's normal completion and error thresholds, increase the rate in explicit steps. I won't prescribe universal numbers because worker capacity and downstream quotas vary. Choose a ceiling below demonstrated spare capacity, write it into the approval, and require another approval to raise it. Compare completion counts with unique business-operation claims rather than delivery attempts; retries make delivery counts look productive while hiding duplicates.

Completion means every selected job has a terminal explanation: completed, safely deduplicated, deliberately excluded, or returned for later review. Reconcile the destination with the immutable preview, attach the report to the incident or change record, and leave the batch queryable for the retention period. Only then should normal retention remove dead-letter records.

The operational recommendation remains plain: reuse the queue's redrive when one team can run this checklist with regional credentials. Add a thin service when multiple queues or teams need the same safety controls. Neither choice compensates for a handler that can't tolerate the same business operation twice.

## References

- Cron — https://en.wikipedia.org/wiki/Cron
- Google Cloud Pub/Sub overview — https://cloud.google.com/pubsub/docs/overview
