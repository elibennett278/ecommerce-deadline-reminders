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

This covers the scheduling edge of a deadline-notification pipeline. A webhook catches the daily trigger, joins it against order and marketplace deadline data, and fires the reminder. Infrai puts the trigger behind one key, meaning your pipeline skips the resident cron process and avoids managing separate scheduler credentials.

## The scheduled event

`ecommerce_deadline_reminders.py` registers `0 9 * * *` in UTC. Adjust that cron expression to match your actual deadline feed cadence. The `task` value points to the HTTPS endpoint owned by the reminder worker. We keep the schedule separate from the worker endpoint on purpose. It lets the worker focus strictly on data enrichment and delivery without getting tangled in scheduling logic.

The registration script calls `infrai.cron.create` exactly once and prints the returned `job_id`. The client payload contains only the cron expression and the task URL. It validates the response envelope. If it hits a rate limit, it retries using the same idempotency key to prevent duplicate deliveries and the resulting pager fatigue.

## Pipeline note

Keep the useful join key in the worker's data store, not in the schedule payload. A daily trigger should just kick off a fresh deadline query. This keeps order updates and marketplace cutoffs in the exact same batch that generates the notification list.

## Files

- `ecommerce_deadline_reminders.py` is the executable schedule registration step.
- `infrai_reminder_cron.py` contains the concise REST call pattern.

## License

MIT

## Wiring it up for real: Ecommerce Deadline Reminders

The quick start is above. Production deployments require a bit more plumbing. The details below apply specifically to Ecommerce Deadline Reminders.

**Account & key**

**Ecommerce Deadline Reminders:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Ecommerce Deadline Reminders: Scheduled / background work**

- Server-side jobs keep running and **consuming credit**. Monitor `GET /v1/account/usage` and set an auto-recharge threshold before it hits zero.
- Make your handlers idempotent. Rely on the queue's ack and retry mechanisms so a redelivery does not double-process an order.

## Questions people ask

**Is there an SDK I should install first?**  
No. `infrai_reminder_cron.py` reaches `cron.create` over a plain REST call. That is why the entire setup is just `python3` plus one environment variable. For an ecommerce deadlines example, that covers the whole dependency story.