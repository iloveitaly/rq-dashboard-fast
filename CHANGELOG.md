# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.2] - 2026-04-05

### Fixed
- Jobs with a non-pickle serializer (e.g. RQ's `JSONSerializer`) caused a 500 error on the job detail page. The dashboard now falls back to JSON deserialization if the default pickle deserialization fails.
- Jobs page pagination showed too many pages on initial load when the stored per-page preference differed from the server default (10). `totalPages` is now computed client-side from the actual per-page value, eliminating phantom pages that returned empty results.
- Jobs sorted globally across all queues instead of per-queue — previously, sorting and pagination were applied independently per queue, producing incorrect ordering and inflated page counts when viewing all queues.

## [0.9.1] - 2026-03-24

### Added
- Queues page card view with per-queue donut charts showing job state breakdown; toggle between card and table layout; scheduler activity bar showing active CronScheduler count
- Confirmation dialog before deleting all jobs in a queue
- Cleanup endpoint (`DELETE /queues/{queue_name}/cleanup`) to remove stale registry entries

### Fixed
- Deleting all jobs in a queue now clears all job registries (failed, finished, started, deferred, scheduled, canceled), not just the queue itself
- Job export included data for the last-processed queue only; now correctly collects across all queues
- Duplicate inner function names in route handlers (caused potential shadowing bugs)
- Stale registry entries were counted in pagination totals but not shown, producing blank pages; cleaned up on fetch
- Auto-refresh replaced `setInterval` with adaptive `setTimeout` to prevent overlapping requests under slow responses

## [0.9.0] - 2026-03-23

### Added
- Schedulers page (`/schedulers`) showing all active `CronScheduler` daemon instances (RQ 2.6+). Each card displays the scheduler name, hostname, PID, last heartbeat with active/stale badge, config file path, and a table of all registered cron jobs with their function name, queue, schedule (cron expression or interval), next run time, and last run time. Autorefresh every 5 seconds via `/schedulers/json`.
- `allow_schedulers` auth config option — set to `false` to hide the Schedulers page and nav link for a token (default: `true`).
- `schedulers` auth config option — list of CronScheduler names a token can see. Supports `["*"]` wildcard (default). Example: `schedulers: ["email-cron"]`.
- `scheduler_visible()` auth utility function, mirroring `worker_visible()`.
- `docs/scheduling.md` — explains the two scheduling mechanisms (one-off `enqueue_at`/`enqueue_in` vs recurring `CronScheduler`), where each appears in the dashboard, and a migration guide from the old `rq-scheduler` package.

### Fixed
- Native RQ scheduled jobs (created with `queue.enqueue_at()` / `queue.enqueue_in()`) were silently dropped and never shown in the Jobs page. The classification loop in `get_job_registrys` was missing an `elif status == "scheduled"` branch. These jobs now correctly appear under the `scheduled` filter.

### Changed
- Minimum RQ version bumped from `>=1.15.1` to `>=2.6.0` to support `CronScheduler.all()`.

### Removed
- Dependency on the third-party `rq-scheduler` package (`rq_scheduler`). The dashboard now relies entirely on RQ's native `ScheduledJobRegistry` and `CronScheduler`. Users still running the old `rq-scheduler` daemon should migrate to `queue.enqueue_at()` / `queue.enqueue_in()` for one-off scheduled jobs, or `CronScheduler` for recurring jobs. See `docs/scheduling.md`.

## [0.8.1] - 2026-03-19

### Added
- Jobs table now defaults to newest-first ordering. For registry-based states (failed, finished, started, deferred, scheduled, canceled) ordering is done server-side using `desc=True` on `get_job_ids`, giving true global newest-first order across all pages. For the queued state, the Redis list is read from the tail and reversed for the same effect.
- Sortable column headers on the jobs table (Name and Created). Clicking a header toggles ascending/descending with a ↑/↓/↕ indicator. Sorting is applied client-side within the returned page, which is the correct behaviour for `state=all` where results are combined from multiple registries.

### Fixed
- `InvalidJobOperation` error when `job.get_status()` is called on a job whose Redis hash has expired or been evicted. Affected jobs are now skipped with a warning log instead of crashing the entire request.
- Redundant double-logging of the same error in Sentry: `get_jobs` re-caught and re-logged the `HTTPException` already raised by `get_job_registrys`. The redundant wrapper has been removed.
- `page` and `perPage` JS variables becoming `undefined` after a failed fetch, causing subsequent autorefresh requests to send `page=undefined&per_page=undefined` (422 loop). Fixed by checking `r.ok` before parsing the response body, and by sanitising the variables at the start of each `updateJobsData` call so any pre-existing corrupted state self-heals.

## [0.8.0] - 2026-03-18

### Added
- Canceled and Stopped job states — jobs in these states now appear in the dashboard instead of being silently dropped (fixes #95)
- `canceled_job_registry` collection and "Canceled" column on the queues page
- "Canceled" and "Stopped" filter options in the jobs state dropdown
- Worker queue filtering — scoped tokens only see workers listening on their allowed queues
- `allow_workers` config option — set to `false` to disable the Workers page and nav link for a token
- `allow_export` config option — set to `false` to disable the Export page and nav link for a token
- `hide_meta` config option — set to `true` to hide the metadata section on the job detail page
- Job detail page: status badge, queue name, action buttons (requeue, delete) with confirmation dialogs
- Job detail page: pretty-printed JSON rendering for dict/list results
- Job detail page: timeline cards for created/enqueued/ended timestamps
- Job detail page: exception block with red styling, shown prominently for failed jobs
- `ended_at` field on job list — shows "Failed/Ended X ago" for completed jobs
- `status` field on `JobDataDetailed` model
- Queue column on the jobs list page
- Relative timestamps ("2m ago", "3d ago") on the jobs page with full date+time on hover
- View button (eye icon) on jobs list to open job details
- Confirmation dialogs on delete and requeue actions
- Skeleton (disabled) buttons for unavailable actions to maintain consistent row alignment
- Badge styles for canceled (orange) and stopped (rose) states
- `btn-view` and `btn-skeleton` button styles

### Changed
- Jobs table redesigned: columns reordered to Name | Status | Queue | Created | Actions
- Job ID moved from primary link column to short ID suffix on job name (full ID on hover)
- Queue filter dropdown moved to Queue column header (was under Name)
- Actions column always visible (view for all users, requeue/delete for admin)
- Job detail page redesigned from flat table to card-based layout
- Empty sections (Result: None, Meta: {}) hidden on job detail page

### Fixed
- Pagination bug: jobs in canceled/stopped state were counted in total but dropped during rendering, causing blank pages at the end
- `raise Exception(status_code=500, ...)` in queues.py and workers.py export helpers — `Exception` does not accept keyword args, causing `TypeError` at runtime. Changed to `HTTPException`.
- Five routes missing `except HTTPException: raise` before generic `except Exception`, which swallowed intentional 403/500 responses
- Queues page SSR links used undefined `{{baseurl}}` Jinja2 variable — changed to `{{prefix}}`
- Queues page SSR links used `?queue=` parameter — changed to `?queue_name=` to match route
- `logger.exception` calls across queues.py, workers.py, and routes using `,` instead of `%s` format specifier
- `ended_at` now included in CSV job export (was silently dropped)
- "Stopped" state filter now works (was missing collection branch in `get_job_registrys`)

## [0.7.2] - 2026-03-16

### Added
- Token-based authentication with per-queue access control (opt-in via YAML config)
- CLI command `rq-dashboard-fast generate-token` to create token/hash pairs
- `--auth-config` flag and `RQ_DASH_AUTH_CONFIG` environment variable
- Per-token custom page title (shown in header and browser tab)
- Read-only and admin access levels with scoped queue visibility
- CSRF protection on all mutation requests (delete, requeue, clear queue)
- Login page shown when auth is enabled and no valid token is present
- Token-to-cookie flow that strips tokens from URLs to prevent leaking
- Authentication documentation in docs/authentication.md

## [0.7.1] - 2026-03-09

### Added
- Dynamic pagination with page size selector (10, 50, 100) on jobs page
- Total job count display in pagination controls
- Loading spinner on action buttons (delete, requeue, clear queue)

## [0.7.0] - 2026-02-27

### Added
- Dark mode with OS detection and manual toggle (persisted in localStorage)
- UnoCSS utility-first CSS framework replacing hand-written CSS
- Inter font served locally (no external Google Fonts requests)
- Color-coded status badges for job states (failed, started, queued, finished, deferred, scheduled)
- SVG icon buttons for delete, requeue, and pagination actions
- Global autorefresh toggle in header (replaces per-page checkboxes)
- Active navigation indicator with underline styling
- Inline table header filters on jobs page (state and queue name)
- Development documentation for UnoCSS workflow in README
- CLAUDE.md with project guidance for AI-assisted development

### Changed
- Replaced jQuery with vanilla JavaScript across all templates
- Redesigned tables with minimal striped style, stronger headers, and row borders
- Modernized button design from solid/outlined to ghost/subtle style
- Moved autorefresh state to localStorage for cross-page persistence
- Sticky header with frosted glass effect
- Sticky footer using flexbox layout

### Removed
- jQuery CDN dependency
- Google Fonts external requests (replaced with local font files)
- Legacy `main.css` (replaced by generated `uno.css` + `custom.css`)
- Per-page autorefresh checkboxes
- Page title headings (active nav link indicates current page)
- Gray card wrapper around page content

### Fixed
- Autorefresh guard blocking manual filter and pagination changes
- `documenr` typo in queues.html error notification
- Variable name mismatch in jobs.html error notification
- `objectName`/`object_name` mismatch in export.html error handler
- Duplicate table cells from broken worker queues loop in workers.html
- Unclosed `<span>` tag in footer.html
- Nested DOCTYPE/html/head/body tags in all child templates
- Filter div incorrectly nested inside table element in jobs.html
- Version mismatch between pyproject.toml and dashboard UI

## [0.6.1] - 2024-12-01

### Added
- CLI entry point (`rq-dashboard-fast` command)
- Standalone mode with `--redis-url`, `--host`, `--port`, `--prefix` flags
- Environment variable support (`REDIS_URL`, `FASTAPI_HOST`, `FASTAPI_PORT`)

## [0.6.0] - 2024-10-01

### Added
- Data export (CSV/JSON) for queues, workers, and jobs
- Export page in dashboard UI

## [0.5.0] - 2024-08-01

### Added
- Pagination on jobs page
- Job requeue functionality

## [0.4.0] - 2024-06-01

### Added
- Workers monitoring page
- Auto-refresh for queues, jobs, and workers pages

## [0.3.0] - 2024-04-01

### Added
- Job detail view with Pygments syntax highlighting for tracebacks
- Job deletion

## [0.2.0] - 2024-03-01

### Added
- Jobs page with state filtering
- Queue name filtering

## [0.1.0] - 2024-02-01

### Added
- Initial release
- FastAPI-based dashboard mountable as sub-application
- Queue monitoring with job count by status
- Docker image support (amd64/arm64)
- PyPI package distribution
