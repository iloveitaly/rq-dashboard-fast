import logging
from datetime import datetime
from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from redis import Redis
from rq.exceptions import InvalidJobOperation
from rq.job import Job
from rq.serializers import JSONSerializer
from rq.utils import as_text

from .auth import queue_allowed
from .queues import get_queues

router = APIRouter()


class JobData(BaseModel):
    id: str
    name: str
    created_at: datetime
    ended_at: datetime | None = None


class JobDataDetailed(BaseModel):
    id: str
    name: str
    status: str | None = None
    created_at: datetime
    enqueued_at: datetime | None
    ended_at: datetime | None
    result: Any
    exc_info: str | None
    meta: dict
    origin: str | None = None


class QueueJobRegistryStats(BaseModel):
    queue_name: str
    scheduled: List[JobData]
    queued: List[JobData]
    started: List[JobData]
    failed: List[JobData]
    deferred: List[JobData]
    finished: List[JobData]
    canceled: List[JobData]
    stopped: List[JobData]


class PaginatedJobResponse(BaseModel):
    data: List[QueueJobRegistryStats]
    total: int
    page: int
    per_page: int
    total_pages: int


logger = logging.getLogger(__name__)


def _filter_valid_job_ids(all_ids: list[str], redis: Redis) -> list[str]:
    """Return only job IDs whose backing hash still exists in Redis."""
    if not all_ids:
        return []
    return [job.id for job in Job.fetch_many(all_ids, connection=redis) if job is not None]


def _sort_ids_by_created_at(job_ids: list[str], redis: Redis, desc: bool = True) -> list[str]:
    """Sort job IDs by created_at using a single Redis pipeline round-trip."""
    if not job_ids:
        return job_ids
    pipeline = redis.pipeline()
    for jid in job_ids:
        pipeline.hget(f"rq:job:{jid}", "created_at")
    raw_values = pipeline.execute()

    def _ts(val) -> float:
        if val is None:
            return 0.0
        try:
            s = val.decode() if isinstance(val, bytes) else val
            return datetime.fromisoformat(s).timestamp()
        except (ValueError, AttributeError):
            return 0.0

    paired = sorted(zip(job_ids, map(_ts, raw_values)), key=lambda x: x[1], reverse=desc)
    return [jid for jid, _ in paired]


def get_job_registrys(
    redis_url: str,
    queue_name: str = "all",
    state: str = "all",
    page: int = 1,
    per_page: int = 10,
    allowed_queues: Optional[list[str]] = None,
    sort_by: str = "created_at",
    sort_dir: str = "desc",
) -> PaginatedJobResponse:
    try:
        redis = Redis.from_url(redis_url)
        queues = get_queues(redis_url)

        desc = sort_dir == "desc"

        # Phase 1: collect all IDs across all matching queues globally
        all_ids: list[str] = []
        for queue in queues:
            if allowed_queues and not queue_allowed(queue.name, allowed_queues):
                continue
            if queue_name != "all" and queue_name != queue.name:
                continue

            if state == "all":
                ids = []
                ids.extend(queue.get_job_ids())
                ids.extend(queue.finished_job_registry.get_job_ids())
                ids.extend(queue.failed_job_registry.get_job_ids())
                ids.extend(queue.started_job_registry.get_job_ids())
                ids.extend(queue.deferred_job_registry.get_job_ids())
                ids.extend(queue.scheduled_job_registry.get_job_ids())
                ids.extend(queue.canceled_job_registry.get_job_ids())
                all_ids.extend(_filter_valid_job_ids(ids, redis))
            elif state == "scheduled":
                all_ids.extend(_filter_valid_job_ids(queue.scheduled_job_registry.get_job_ids(), redis))
            elif state == "queued":
                all_ids.extend([as_text(jid) for jid in redis.lrange(queue.key, 0, -1)])
            elif state == "finished":
                all_ids.extend(_filter_valid_job_ids(queue.finished_job_registry.get_job_ids(), redis))
            elif state == "failed":
                all_ids.extend(_filter_valid_job_ids(queue.failed_job_registry.get_job_ids(), redis))
            elif state == "started":
                all_ids.extend(_filter_valid_job_ids(queue.started_job_registry.get_job_ids(), redis))
            elif state == "deferred":
                all_ids.extend(_filter_valid_job_ids(queue.deferred_job_registry.get_job_ids(), redis))
            elif state == "canceled":
                all_ids.extend(_filter_valid_job_ids(queue.canceled_job_registry.get_job_ids(), redis))
            elif state == "stopped":
                # Stopped jobs live in started_job_registry with status "stopped".
                # Filter them from the started registry by checking actual status.
                for jid in Job.fetch_many(queue.started_job_registry.get_job_ids(), connection=redis):
                    if jid is not None and jid.get_status() == "stopped":
                        all_ids.append(jid.id)

        total = len(all_ids)

        # Phase 2: global sort and single page slice
        start_index = (page - 1) * per_page
        end_index = start_index + per_page
        page_ids = _sort_ids_by_created_at(all_ids, redis, desc=desc)[start_index:end_index]

        # Phase 3: fetch page jobs and group by queue for the response structure
        queue_data: dict[str, dict] = {}
        for job in Job.fetch_many(page_ids, connection=redis):
            if job is None:
                continue
            try:
                status = job.get_status()
            except InvalidJobOperation:
                logger.warning("Skipping job %s: status no longer available in Redis", job.id)
                continue
            qname = job.origin or "unknown"
            if qname not in queue_data:
                queue_data[qname] = {s: [] for s in ("started", "failed", "deferred", "finished", "queued", "scheduled", "canceled", "stopped")}
            job_data_item = JobData(
                id=job.id,
                name=job.description,
                created_at=job.created_at,
                ended_at=job.ended_at,
            )
            if status in queue_data[qname]:
                queue_data[qname][status].append(job_data_item)

        result = [
            QueueJobRegistryStats(
                queue_name=qname,
                scheduled=data["scheduled"],
                queued=data["queued"],
                started=data["started"],
                failed=data["failed"],
                deferred=data["deferred"],
                finished=data["finished"],
                canceled=data["canceled"],
                stopped=data["stopped"],
            )
            for qname, data in queue_data.items()
        ]

        total_pages = max(1, (total + per_page - 1) // per_page)
        return PaginatedJobResponse(
            data=result,
            total=total,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
        )
    except Exception as error:
        logger.exception("Error fetching job registries: %s", error)
        raise HTTPException(
            status_code=500, detail=str("Error fetching job registries")
        )


def get_jobs(
    redis_url: str,
    queue_name: str = "all",
    state: str = "all",
    page: int = 1,
    per_page: int = 10,
    allowed_queues: Optional[list[str]] = None,
    sort_by: str = "created_at",
    sort_dir: str = "desc",
) -> PaginatedJobResponse:
    return get_job_registrys(
        redis_url,
        queue_name,
        state,
        page,
        per_page,
        allowed_queues=allowed_queues,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )


def _fetch_job_result(job: Job) -> Any:
    """Fetch job result, falling back to JSON deserialization if Pickle fails."""
    try:
        return job.result
    except Exception:
        pass
    try:
        result = job.latest_result(serializer=JSONSerializer)
        return result.return_value if result else None
    except Exception:
        return None


def get_job(redis_url: str, job_id: str) -> JobDataDetailed:
    try:
        redis = Redis.from_url(redis_url)
        job = Job.fetch(job_id, connection=redis)

        return JobDataDetailed(
            id=job.id,
            name=job.description,
            status=job.get_status(),
            created_at=job.created_at,
            enqueued_at=job.enqueued_at,
            ended_at=job.ended_at,
            result=_fetch_job_result(job),
            exc_info=job.exc_info,
            meta=job.meta,
            origin=job.origin,
        )
    except Exception as error:
        logger.exception("Error fetching job: %s", error)
        raise HTTPException(status_code=500, detail=str("Error fetching job"))


def delete_job_id(redis_url: str, job_id: str):
    try:
        redis = Redis.from_url(redis_url)
        job = Job.fetch(job_id, connection=redis)
        if job:
            job.delete()
    except Exception as error:
        logger.exception("Error deleting specific job: %s", error)
        raise HTTPException(status_code=500, detail=str("Error deleting specific job"))


def requeue_job_id(redis_url: str, job_id: str):
    try:
        redis = Redis.from_url(redis_url)
        job = Job.fetch(job_id, connection=redis)
        if job:
            job.requeue()
    except Exception as error:
        logger.exception("Error reloading specific job: %s", error)
        raise HTTPException(status_code=500, detail=str("Error reloading specific job"))


def convert_queue_job_registry_stats_to_json_dict(
    job_data: List[QueueJobRegistryStats],
) -> list[dict]:
    try:
        job_stats_dict = {}

        for job_stats in job_data:

            def job_data_to_dict(job_data: JobData):
                d = {
                    "id": job_data.id,
                    "name": job_data.name,
                    "created_at": job_data.created_at.isoformat(),
                }
                if job_data.ended_at is not None:
                    d["ended_at"] = job_data.ended_at.isoformat()
                return d

            stats_dict = {
                "scheduled": [job_data_to_dict(job) for job in job_stats.scheduled],
                "queued": [job_data_to_dict(job) for job in job_stats.queued],
                "started": [job_data_to_dict(job) for job in job_stats.started],
                "failed": [job_data_to_dict(job) for job in job_stats.failed],
                "deferred": [job_data_to_dict(job) for job in job_stats.deferred],
                "finished": [job_data_to_dict(job) for job in job_stats.finished],
                "canceled": [job_data_to_dict(job) for job in job_stats.canceled],
                "stopped": [job_data_to_dict(job) for job in job_stats.stopped],
            }
            job_stats_dict[job_stats.queue_name] = stats_dict

        queue_stats_list = [job_stats_dict]
        return queue_stats_list
    except Exception as error:
        logger.exception(
            "Error converting queue job registry stats list to JSON dictionary: %s",
            error,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Error converting queue job registry stats list to JSON dictionary",
        )


def cleanup_stale_jobs(
    redis_url: str,
    queue_name: str = "all",
    allowed_queues: Optional[list[str]] = None,
) -> dict:
    """Remove job IDs from registries whose backing hash no longer exists in Redis."""
    try:
        redis = Redis.from_url(redis_url)
        queues = get_queues(redis_url)
        total_removed = 0

        for queue in queues:
            if allowed_queues and not queue_allowed(queue.name, allowed_queues):
                continue
            if queue_name != "all" and queue_name != queue.name:
                continue

            for registry in [
                queue.finished_job_registry,
                queue.failed_job_registry,
                queue.started_job_registry,
                queue.deferred_job_registry,
                queue.scheduled_job_registry,
                queue.canceled_job_registry,
            ]:
                all_ids = registry.get_job_ids()
                if not all_ids:
                    continue
                jobs = Job.fetch_many(all_ids, connection=redis)
                stale_ids = [jid for jid, job in zip(all_ids, jobs) if job is None]
                if stale_ids:
                    redis.zrem(registry.key, *stale_ids)
                    total_removed += len(stale_ids)
                    logger.info(
                        "Removed %d stale job IDs from %s for queue %s",
                        len(stale_ids),
                        registry.__class__.__name__,
                        queue.name,
                    )

        return {"removed": total_removed}
    except Exception as error:
        logger.exception("Error cleaning up stale jobs: %s", error)
        raise HTTPException(status_code=500, detail="Error cleaning up stale jobs")


def convert_queue_job_registry_dict_to_list(input_data: list[dict]) -> list[dict]:
    job_details = []
    try:
        for queue_dict in input_data:
            for queue_name, queue_data in queue_dict.items():
                for status, jobs in queue_data.items():
                    for job in jobs:
                        job_info = {
                            "id": job["id"],
                            "queue_name": queue_name,
                            "status": status,
                            "job_name": job["name"],
                            "created_at": job["created_at"],
                            "ended_at": job.get("ended_at", ""),
                        }
                        job_details.append(job_info)
        return job_details
    except Exception as error:
        logger.exception("Error converting job registry stats dict to list: %s", error)
        raise HTTPException(
            status_code=500,
            detail=f"Error converting job registry stats dict to list",
        )
