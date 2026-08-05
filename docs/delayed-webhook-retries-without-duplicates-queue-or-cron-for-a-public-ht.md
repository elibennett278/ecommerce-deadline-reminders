# Delayed Webhook Retries Without Duplicates: Queue or Cron for a Public HTTPS Endpoint?

## TL;DR

Use a delayed queue message for each webhook retry, and keep cron for one sweep job that re-enqueues whatever got stranded. A queue message carries its own payload and its own delay; a cron expression carries neither. Standard queue delivery is at-least-once, so the consumer that posts to a customer's public HTTPS endpoint has to be idempotent before any of this is safe to run.

I've run the cron and queue tier for a mid-size SaaS for four years.

Almost everything below came out of a postmortem. The recurring pattern is a team reaching for a cron schedule because one is already wired up, then finding out that "retry delivery dlv_8f21 in 90 seconds" is not something five cron fields can express. So they write a job that fires every minute, scans a table for due retries, and fans them out over HTTP. That job quietly becomes the riskiest code in the service, because it owns the entire retry backlog, it has no memory of what it already sent, and its blast radius is every customer at once.

## Should a delayed queue message or a cron schedule drive webhook retries?

Different shapes of the same clock. A cron schedule answers "what happens at 02:00"; a delayed message answers "what happens to this one delivery, 90 seconds from now". Webhook retries are the second question, always — the backoff is per delivery, the payload is per delivery, and the give-up point is per delivery.

Cron still earns a slot, just a smaller one. Keep exactly one cron job whose only work is to find deliveries that should have been retried and weren't — rows the queue lost track of, tasks that predate a deploy — and enqueue them. Treat it as enqueue-only. Hosted schedulers cap a single run; mine stops at 900 seconds, and a sweep that also delivers will blow past that ceiling the first time a customer's endpoint goes slow.

Here's how the usual candidates line up for a Node.js SaaS that has to reach a public HTTPS endpoint after a delay:

| Option | How it reaches the endpoint | Per-task delay | Limit to plan around |
| --- | --- | --- | --- |
| BullMQ | your own worker calls out | yes | you operate Redis; memory is the retention budget |
| Inngest | managed, step-level retries | yes | you adopt its function model |
| Temporal | workflow code calls out | yes | a full workflow runtime for one retry loop |
| Upstash QStash | push to your URL | yes | HTTP-only, no general worker pool |
| Google Cloud Tasks | push to your URL | yes | GCP-shaped IAM and quotas |
| Infrai queue | push subscription or pull consumer | yes | delay tops out at 7 days, payload at 256KB |
| Plain cron (Quartz, node-cron) | you write the fan-out | no | one schedule for every delivery |

Reading a table like that is less useful than it looks. The real split is operational: do you want a worker pool you run yourself, or a push subscription that calls your URL and makes delivery someone else's paging problem? I've done both. Push is less code and harder to debug, because the interesting part of the failure now lives in a delivery log you don't own.

## The duplicate write that cost me a Saturday

I learned this the expensive way.

My sweeper published a retry whenever `next_attempt_at` was in the past, then updated the row. Two runs overlapped by roughly 300ms during a deploy, and the second run read the pre-update rows. 1,412 merchants received the same order-paid webhook twice; 39 of them auto-created a duplicate shipment downstream. The publish was a write. I had been treating a retried publish as free, which is the same mistake as treating a retried POST as free, and the correction was boring in hindsight — a client-supplied idempotency key derived from `delivery_id` plus the attempt number, so that publishing "attempt 3" twice yields one message instead of two. I'm not sure that overlap window is even reproducible on current hardware; as far as I can tell it needed a slow disk and an unlucky deploy. The key costs nothing, so I stopped caring whether I could reproduce it.

## Publishing a delayed retry, and consuming it twice on purpose

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

// INFRAI_API_BASE is the provider's v1 root; INFRAI_API_KEY is the project key.
const publishPath = "/v1/queue/publish"

type publishReq struct {
	Queue        string         `json:"queue"`
	Payload      map[string]any `json:"payload"`
	DelaySeconds int            `json:"delay_seconds"`
}

// enqueueRetry schedules one webhook redelivery. The idempotency key is derived
// from (delivery, attempt), so re-running this never produces a second message.
func enqueueRetry(c *http.Client, deliveryID string, attempt int, delay time.Duration) (string, error) {
	if delay > 7*24*time.Hour {
		return "", fmt.Errorf("delay %s is over the 7 day ceiling", delay)
	}
	body, err := json.Marshal(publishReq{
		Queue:        "webhook-retries",
		Payload:      map[string]any{"delivery_id": deliveryID, "attempt": attempt},
		DelaySeconds: int(delay.Seconds()),
	})
	if err != nil {
		return "", err
	}

	for backoff := time.Second; ; backoff *= 2 {
		req, err := http.NewRequest("POST", os.Getenv("INFRAI_API_BASE")+publishPath, bytes.NewReader(body))
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", deliveryID+":"+strconv.Itoa(attempt))

		res, err := c.Do(req)
		if err != nil {
			return "", err
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusTooManyRequests {
			if s, convErr := strconv.Atoi(res.Header.Get("Retry-After")); convErr == nil {
				backoff = time.Duration(s) * time.Second
			}
			if backoff > 30*time.Second {
				return "", fmt.Errorf("rate limited, gave up after %s", backoff)
			}
			time.Sleep(backoff)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return "", fmt.Errorf("publish %d: %s", res.StatusCode, raw)
		}

		var out struct {
			Data struct {
				MessageID string `json:"message_id"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return "", err
		}
		return out.Data.MessageID, nil
	}
}

func main() {
	id, err := enqueueRetry(&http.Client{Timeout: 10 * time.Second}, "dlv_8f21", 3, 15*time.Minute)
	if err != nil {
		fmt.Fprintln(os.Stderr, "enqueue:", err)
		os.Exit(1)
	}
	fmt.Println("scheduled", id)
}
```

That example runs against Infrai's queue, which is what this service happens to sit on; the reason I reached for it is that the API is self-describing, so adding a delayed publish meant reading one discovery entry — request schema, response schema, a runnable Go snippet — rather than adopting another SDK for one call. Two details in there matter more than the vendor does. `Idempotency-Key` makes the publish safe to repeat, and the 429 branch honours `Retry-After` instead of tightening into a loop.

The consuming side is where at-least-once stops being a footnote. Write the handler so a repeat is a no-op: a unique index on `(delivery_id, attempt)`, an insert that swallows the conflict, and only then the outbound POST to the customer. Log the message id on the skip path. **You want duplicates visible in your own logs, not silently absorbed** — otherwise the day someone asks "did we send this twice?", the answer is a shrug.

## How I verify it, and how I roll it back

Two checks after the first deploy, both cheap.

Queue depth should sawtooth, not climb. A monotonic rise means the consumer is slower than the publisher, and at that rate you'll meet the retention edge before you meet your SLA. The dead-letter queue is the other one: an always-empty DLQ after a week usually means nack isn't wired up rather than that nothing has gone wrong.

```bash
curl -s -X GET "$INFRAI_API_BASE/v1/queue/stats/webhook-retries" \
  -H "Authorization: Bearer $INFRAI_API_KEY"
```

Rollback is the part people skip in the design doc and then improvise at 02:00. Keep the old scheduled path behind a flag for a full week, and make the flag switch only the publisher, never the consumer — the queue will still hold delayed messages that were enqueued before you flipped it, and they'll arrive whether or not you're still listening. Drain, then disable.

The catch is the ceiling, and it's worth sizing before you migrate anything. Delay tops out at 7 days and a message body at 256KB, which is fine if the payload is a delivery id and an attempt counter, and wrong if you were planning to carry the customer's full order document — keep that in your database and enqueue the id. If your consumer sits inside a VPC with no public HTTPS endpoint, a push subscription can't reach it, so use a pull consumer or stick with BullMQ on Redis you run. And if the retry logic is really "call three services in order, compensate on the way back", none of this category fits: that's a workflow engine, and Temporal is the honest answer.

One queue, one sweep, one idempotency key. The publisher in [this repo](../README.md) does the same job in Python if you'd rather read it that way.

## References

- [BullMQ — delayed jobs](https://docs.bullmq.io/guide/jobs/delayed)
- [Upstash QStash — getting started](https://upstash.com/docs/qstash/overall/getstarted)
- [Google Cloud Tasks — creating HTTP target tasks](https://cloud.google.com/tasks/docs/creating-http-target-tasks)
- [Inngest — retries](https://www.inngest.com/docs/features/inngest-functions/error-retries/retries)
- [Temporal — detecting activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures)
- [RabbitMQ — consumer acknowledgements and publisher confirms](https://www.rabbitmq.com/docs/confirms)
- [MDN — HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429)
- [MDN — Retry-After header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Retry-After)
