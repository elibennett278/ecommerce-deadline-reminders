"""Schedule a daily reminder webhook for an e-commerce deadline feed."""

import os

import infrai_reminder_cron as infrai


def schedule_deadline_reminders() -> str:
    """Register a 09:00 UTC reminder job and return its job id."""
    task_url = os.environ["REMINDER_WEBHOOK_URL"]
    schedule = infrai.cron.create(
        cron_expr="0 9 * * *",
        task=task_url,
    )
    return schedule["job_id"]


if __name__ == "__main__":
    job_id = schedule_deadline_reminders()
    print(f"Reminder schedule created: {job_id}")
