import logging
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel
from redis import Redis

from rq_dashboard_fast.utils.auth import scheduler_visible

logger = logging.getLogger(__name__)

STALE_THRESHOLD_SECONDS = 120


class CronJobData(BaseModel):
    func_name: str
    queue_name: str
    interval: Optional[int] = None
    cron: Optional[str] = None
    next_enqueue_time: Optional[datetime] = None
    latest_enqueue_time: Optional[datetime] = None


class SchedulerData(BaseModel):
    name: str
    hostname: str
    pid: int
    created_at: datetime
    config_file: str
    last_heartbeat: Optional[datetime] = None
    is_stale: bool
    jobs: list[CronJobData]


def get_schedulers(redis_url: str, allowed_schedulers: list[str]) -> list[SchedulerData]:
    try:
        from rq.cron import CronScheduler
    except ImportError:
        logger.warning("rq.cron.CronScheduler not available — RQ >= 2.6 required")
        return []

    redis = Redis.from_url(redis_url)

    try:
        schedulers = CronScheduler.all(connection=redis, cleanup=False)
    except Exception:
        logger.exception("Failed to fetch CronSchedulers")
        return []

    now = datetime.now(tz=timezone.utc)
    result = []

    for scheduler in schedulers:
        last_hb = scheduler.last_heartbeat
        if last_hb is None:
            is_stale = True
        else:
            is_stale = (now - last_hb).total_seconds() > STALE_THRESHOLD_SECONDS

        jobs = [
            CronJobData(
                func_name=job.func_name,
                queue_name=job.queue_name,
                interval=job.interval,
                cron=job.cron,
                next_enqueue_time=job.next_enqueue_time,
                latest_enqueue_time=job.latest_enqueue_time,
            )
            for job in scheduler.get_jobs()
            if scheduler_visible(job.queue_name, allowed_schedulers)
        ]

        if not jobs and "*" not in allowed_schedulers:
            continue

        result.append(
            SchedulerData(
                name=scheduler.name,
                hostname=scheduler.hostname,
                pid=scheduler.pid,
                created_at=scheduler.created_at,
                config_file=scheduler.config_file,
                last_heartbeat=last_hb,
                is_stale=is_stale,
                jobs=jobs,
            )
        )

    return result
