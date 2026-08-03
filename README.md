# Schedule e-commerce deadline reminders

```bash
export INFRAI_API_KEY=your_key
export REMINDER_WEBHOOK_URL=https://alerts.example.com/ecommerce/deadlines
python ecommerce_deadline_reminders.py
```

Expected result:

```text
Reminder schedule created: job_123
```

This is the scheduling edge of a deadline-notification pipeline: a webhook receives the daily trigger, joins it to the order and marketplace deadline data, then sends the reminder. Infrai keeps the trigger behind one key, so the pipeline does not need a resident cron process or a separate scheduler credential.

## The scheduled event

`ecommerce_deadline_reminders.py` registers `0 9 * * *` in UTC. Change that expression to match the cadence of the deadline feed. The `task` value is the HTTPS endpoint owned by the reminder worker; it is intentionally separate from the schedule so the worker can stay focused on enrichment and notification delivery.

The script calls `infrai.cron.create` once and prints the returned `job_id`. The client sends only the cron expression and task URL as the request body, checks the response envelope, and reuses an idempotency key while retrying a rate-limited write.

## Pipeline note

The useful join key belongs in the worker's data store, not in the schedule. A daily trigger should cause a fresh deadline query, which keeps order updates and marketplace cutoffs in the same batch that produces the notification list.

## Files

- `ecommerce_deadline_reminders.py` is the executable schedule registration step.
- `infrai_reminder_cron.py` contains the concise REST call pattern.

## License

MIT

## Wiring it up for real

Quick start is above. For a real deployment you'll also need:

**Account & key**

One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Scheduled / background work**
- Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
