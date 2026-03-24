# Scheduled Jobs

RQ has two distinct scheduling mechanisms, and the dashboard displays each in a different place.

## One-off scheduled jobs — Jobs page

Jobs scheduled to run at a specific future time using RQ's built-in scheduling API appear in the **Jobs page** under the `scheduled` status filter.

```python
from rq import Queue
from datetime import datetime, timedelta, timezone

q = Queue(connection=redis)

# Run at a specific time
q.enqueue_at(datetime(2025, 12, 31, 23, 59, tzinfo=timezone.utc), my_func)

# Run after a delay
q.enqueue_in(timedelta(minutes=30), my_func)
```

These jobs are stored in RQ's native `ScheduledJobRegistry`. A separate RQ scheduler process must be running to move them into the queue when their time arrives:

```bash
rq scheduler -u redis://localhost:6379
```

Until a scheduler process picks them up, they sit in the registry and appear as `scheduled` in the Jobs page. Once enqueued they transition to `queued` and proceed normally.

## Recurring cron jobs — Schedulers page

Long-running `CronScheduler` daemon processes (RQ 2.4+) appear in the **Schedulers page**. Each card shows the scheduler's name, host, PID, heartbeat status, and the list of registered recurring jobs with their cron expression or interval.

```python
from rq.cron import CronScheduler

cron = CronScheduler(connection=redis, name="my-scheduler")
cron.register(send_digest, queue_name="emails", cron="0 9 * * *")
cron.register(sync_data,   queue_name="default", interval=300)
cron.start()
```

Each time the scheduler fires a job it is enqueued normally, so the resulting job execution is visible in the **Jobs page** like any other job.

A scheduler badge shows **active** (green) while heartbeats are current, or **stale** (red) if no heartbeat has been received in the last 120 seconds, which typically means the process has crashed or been stopped.

## Summary

| Mechanism | API | Where it appears |
|-----------|-----|-----------------|
| One-off future job | `queue.enqueue_at()` / `queue.enqueue_in()` | Jobs page — `scheduled` filter |
| Recurring cron job | `CronScheduler.register()` | Schedulers page |

## Migrating from rq-scheduler

The third-party [`rq-scheduler`](https://github.com/rq/rq-scheduler) package predates RQ's built-in scheduling. As of rq-dashboard-fast 0.9.0, support for the old package has been removed in favour of native RQ scheduling. If you are still using `rq-scheduler`:

- Replace `scheduler.enqueue_at(dt, func)` with `queue.enqueue_at(dt, func)`
- Replace `scheduler.enqueue_in(td, func)` with `queue.enqueue_in(td, func)`
- Replace periodic jobs with `CronScheduler.register(func, queue_name=..., cron=...)`

See the [RQ scheduling docs](https://python-rq.org/docs/scheduling/) and [RQ cron docs](https://python-rq.org/docs/cron/) for full details.
