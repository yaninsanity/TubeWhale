## Docker environment loading

Single-source env: `.env` in the project root.

Behavior:
- If `.env` exists, Compose will load it automatically and override in-file defaults.
- If `.env` is missing, `start-docker.sh` will generate a minimal `.env` with sane defaults to reduce setup friction.

Quick start:
- ./start-docker.sh start
- To include Nginx: ./start-docker.sh start --with-nginx

### Ultra-fast Docker Dev (Makefile)

Added a lightweight `Makefile` with the most common lifecycle commands:

```bash
# 1. Build images (backend, worker, beat)
make build

# 2. Start core stack (db + redis + backend + workers)
make up

# 3. Apply migrations
make migrate

# 4. Seed 9 Expert Prompts + Core Templates (idempotent)
make seed

# 5. Create superuser
make superuser

# 6. Tail backend logs
make logs

# 7. Open a shell inside backend container
make shell

# 8. Restart backend only
make restart

# 9. Collect static (on demand)
make collectstatic

# 10. Dangerous: stop & remove volumes
make wipe
```

After `make seed`, you should see exactly 9 English Expert Prompts and the core templates in the admin panel.

If prompts do not appear, confirm the container is rebuilt (cache), then rerun:
```bash
docker compose exec backend python manage.py seed_templates_and_prompts --apply --force
```

`--force` (if added later) can be used to overwrite diverged content; today the command performs safe reconciliation (updates changed fields, creates missing, leaves unknown extras).

### Prompt & Template Catalog APIs (English Only)

Read‑only endpoints for CLI / external automation:

| Endpoint | Purpose |
|----------|---------|
| `GET /templates/expert/catalog/` | List expert prompts (count + array) |
| `GET /templates/core/catalog/` | List core summary templates |
| `GET /templates/custom-templates/` | DRF viewset (auth) full CRUD for custom templates |
| `GET /templates/custom-templates/domains/` | Engine domains |
| `POST /templates/custom-templates/compile/` | Compile template with variables |

Query params: `active_only=1` filters inactive expert prompts.

Environment flag: `FORCE_EN_PROMPTS=true` ensures English context (default true).

### Management Commands

```bash
# Seed (idempotent)
python manage.py seed_templates_and_prompts

# Force overwrite canonical fields
python manage.py seed_templates_and_prompts --force

# Export (stdout)
python manage.py export_prompts_templates

# Export all (including inactive + all template types) to file
python manage.py export_prompts_templates --include-inactive --all-templates -f prompts_templates.json
```

### Accessibility Practices

Admin template management views include:
- Semantic landmarks (`role="main"`, search form roles, table headers with `scope="col"`)
- ARIA labels for interactive import/preview actions (extend as needed)
- English-only enforcement for consistent operator UX

See `apps/templates_app/templates/admin/ACCESSIBILITY_NOTES.md` for expansion roadmap.

### Real-Time System Metrics (Admin)

High-frequency operational observability for staff users:

- Dashboard: `/templates/admin/system/metrics/` (Chart.js driven, 5s polling)
- JSON Endpoint: `/templates/admin/system/metrics.json` (lightweight payload for custom dashboards)
- SSE Stream: `/templates/admin/system/metrics.stream` (Server-Sent Events push every 5s)
- Prometheus: `/templates/admin/system/metrics.prom` (plain text exposition format)
- Hotspot Events: `/templates/admin/system/hotspots/` sustained critical resource periods
- Data Provided: CPU %, Memory %, Load Avg (1m/5m/15m), Disk %, Disk GB used/total, Process RSS MB
- Rolling History: ~10 minutes (in-process ring buffer; resets on restart)
- Thresholds (env configurable): `CPU_WARN_THRESHOLD` `CPU_CRIT_THRESHOLD` `MEM_WARN_THRESHOLD` `MEM_CRIT_THRESHOLD` `HOTSPOT_MIN_DURATION_SEC`
- Accessible: Charts `role="img"`; live summary `role="status" aria-live="polite"`; textual badges for warnings.
- Hard Dependency: `psutil>=5.9,<6.0` required - application will not start without it.

Use Cases:
- Quick spike detection (CPU, memory saturation)
- Capacity planning (sustained load vs. resource ceiling)
- Triaging performance regressions during deployments.
- Historical hotspot triage (sustained critical events logged) 

### Upgrading to Metrics + Hotspot Version

1. Pull latest code & rebuild images:
    - `docker compose build --no-cache backend`
2. Apply migrations (adds `SystemHotspotEvent`):
    - `docker compose exec backend python manage.py migrate`
3. (Optional) Tune thresholds via `.env`:
```
CPU_WARN_THRESHOLD=70
CPU_CRIT_THRESHOLD=90
MEM_WARN_THRESHOLD=75
MEM_CRIT_THRESHOLD=90
HOTSPOT_MIN_DURATION_SEC=30
```
4. Visit `/templates/admin/system/metrics/` → switch SSE / Polling.
5. View hotspot history: `/templates/admin/system/hotspots/`.

Prometheus scrape target example:
```
- job_name: 'tubewhale'
   static_configs:
      - targets: ['backend:8000']
   metrics_path: /templates/admin/system/metrics.prom
```

# TubeWhale 🐳✨
``` bash
88888888888       888               888       888 888    888        d8888 888      8888888888 
    888           888               888   o   888 888    888       d88888 888      888        
    888           888               888  d8b  888 888    888      d88P888 888      888        
    888  888  888 88888b.   .d88b.  888 d888b 888 8888888888     d88P 888 888      8888888    
    888  888  888 888 "88b d8P  Y8b 888d88888b888 888    888    d88P  888 888      888        
    888  888  888 888  888 88888888 88888P Y88888 888    888   d88P   888 888      888        
    888  Y88b 888 888 d88P Y8b.     8888P   Y8888 888    888  d8888888888 888      888        
    888   "Y88888 88888P"   "Y8888  888P     Y888 888    888 d88P     888 88888888 8888888888 
```                                                                                  
                                                                                              
                                                                                              
TubeWhale is a fun, open-source, AI-powered multi-agent video processing system designed to search for and analyze YouTube videos efficiently! 🚀 Although the pipeline is currently runnable, there are still a few engineering improvements to be made to ensure its robustness. 🛠️

### Project Status: 🟢


![Logo](logo.png)

![star-history](star-history-2024109.png)

### TubeWhale – An Enhanced AI Product Documentation for Multi-Agent Keyword Brainstorming and Video Analysis

## 1. Introduction:
TubeWhale is an open-source AI-powered multi-agent video processing system designed to search for and analyze YouTube videos efficiently. By leveraging keyword brainstorming, video metadata collection, and multimodal analysis (including audio transcription), the system provides intelligent summaries and insights into video content. It is especially suited for research and use cases where automatic topic generation and summarization are essential.💡

Key Differentiator: TubeWhale employs multiple AI agents to brainstorm topic keywords and searches for YouTube videos based on those keywords. Users have control over the number of videos analyzed, ensuring precision and flexibility tailored to their specific needs.🎯

### Flow Chart
![flow-chart](flow-chart.png)

### Features
- 🔑 Keyword Brainstorming: AI agents expand your base keyword into multiple variations.
- 🎥 YouTube Search & Metadata: Fetches top‑k videos per keyword, deduplicates, scores by view/like/comment.
- 📝 Transcription & Summaries: Uses YouTube captions → Whisper fallback → GPT‑4 summarization.
- 🎙️ Audio Analysis: Optional full‑audio Whisper transcription + GPT summarization + chunked‑fallback.
- 💾 Persistence: Stores metadata, transcripts, summaries, comments, logs in SQLite.
- 📊 Report: Generates a JSON report with token usage and cost.
- ⚙️ Highly Configurable: All parameters via .env or CLI flags.

## 1. Key Concepts and Parameters
When running the system, the user can customize various parameters that control how the pipeline operates:

```bash
python3 cli.py 
```
You will receive a database with max `MAX_N` * `TOP_K` videos. This videos list will be deduplicated.

## Key Concepts and Configuration & Parameter Explanations:

### TubeWhale is highly configurable through environment variables. Below are the key parameters and their explanations to help you tailor the system to your requirements.

# 2.Configuration ⚙️
Create a `.env` in project root:
```bash
# YouTube API keys (comma‑separated)
# API keys
YOUTUBE_API_KEYS="AIzaSyXXX,AIzaSyYYY,AIzaSyZZZ"
OPENAI_API_KEY="sk-..."

# Pipeline flags
FULL_AUDIO_ANALYSIS=true
PERSIST_AGENT_SUMMARIES=true
DRY_RUN=false
PURE_YOUTUBE=false

# Search & analysis
KEYWORD="Arizona homeless during covid19"
MAX_N=5
TOP_K=3
FILTER_TYPE="view_count"

# Storage & concurrency
DB_PATH="youtube_summaries.db"
CONCURRENCY=1
```

| Variable                          | Description                                                  |
| --------------------------------- | ------------------------------------------------------------ |
| KEYWORD (required)                | Base search term for keyword brainstorming.                 |
| MAX_N (default=5)                 | Number of keyword variations to generate.                   |
| TOP_K (default=3)                 | Number of videos fetched per variation.                     |
| FILTER_TYPE (default=view_count)  | How to sort/filter videos (view_count, like_count, etc.).   |
| FULL_AUDIO_ANALYSIS (default=true)| Enable Whisper + GPT audio processing.                      |
| PERSIST_AGENT_SUMMARIES (default=true) | Store transcript summaries and audio summaries.       |
| DRY_RUN (default=false)           | No external API calls or DB writes — for testing.           |
| PURE_YOUTUBE (default=false)      | Pure YouTube mode: disable AI expansion and audio analysis. |
| DB_PATH (default=youtube_summaries.db) | SQLite database file path.                              |
| CONCURRENCY (default=1)           | Number of parallel video processing tasks.                  |

# 3. Environment Setup
Requirements
Python Version >=3.11.x
```bash
git clone https://github.com/yaninsanity/TubeWhale.git
cd TubeWhale
python3 -m venv venv
source venv/bin/activate
# install torch cpu 
pip3 install --pre torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/nightly/cpu
pip install pip --upgrade
pip install -r requirements.txt
python3 cli.py
```

Additionally, install FFmpeg:
On macOS: `brew install ffmpeg`
On Linux: `sudo apt install ffmpeg`


# 4 How Tubewhale🐳 Works

1. **Keyword Brainstorming**  
   GPT‑4 agents generate `MAX_N` variations of your base `KEYWORD`.

2. **YouTube Search**  
   - Fetch the top `TOP_K` videos for each keyword variation.  
   - Deduplicate results.  
   - Score videos by view count, likes, comments (per `FILTER_TYPE`).

3. **Metadata Storage**  
   Store each video’s metadata in the `videos` table.

4. **Transcription & Summaries**  
   1. **Try YouTube captions** via `TranscriptAgent`.  
   2. **Fallback to Whisper** for full‑audio transcription.  
   3. **GPT Summarization**  
      - Save raw transcripts and generated summaries in the `transcripts` table.  
      - Log prompt/response, tokens and cost in `ai_interactions`.

5. **Audio Analysis** (optional; if `FULL_AUDIO_ANALYSIS=true`)  
   - **Mode A:** Full‑audio → Whisper → GPT summary.  
   - **Mode B (fallback):**  
     1. Slice audio into 60 s chunks.  
     2. Run Whisper + GPT on each chunk.  
     3. Recursively merge chunk summaries into one final summary.

6. **Comments**  
   Fetch all comments via YouTube API and save them in the `comments` table.

7. **Standardization**  
   Normalize each summary to the JSON schema using `StandardizerAgent`.

8. **Final Report**  
   After all videos are processed, generate `logs/report_<timestamp>.json` containing:  
   - `processed_videos` count  
   - List of `video_ids`  
   - `total_prompt_tokens` and `total_completion_tokens`  
   - `total_cost`  
   - Run `timestamp`


# 5 Usage Examples 🎉

## 5.1 Basic Usage
To run the system with default configuration:
```bash
python3 cli.py 
```

## 5.2 CLI Options
TubeWhale provides extensive CLI options for flexibility:

```bash
# Basic keyword search
python3 cli.py --keyword "machine learning" --top-k 5

# Dry run for testing (no API calls)
python3 cli.py --dry-run --keyword "test" --top-k 1

# Pure YouTube mode (faster, no AI expansion)
python3 cli.py --pure-youtube --top-k 10

# Enable audio analysis
python3 cli.py --audio --keyword "tutorials" --top-k 3

# High concurrency processing
python3 cli.py --concurrency 8 --keyword "python"

# Verbose logging for debugging
python3 cli.py --verbose --keyword "debug test"

# Configuration testing
python3 cli.py --config-test
```

## 5.3 CLI Parameters Reference

| CLI Parameter | Short | Description | Example |
|---------------|-------|-------------|---------|
| `--keyword` | `-k` | Search keyword (overrides env var) | `--keyword "AI tutorial"` |
| `--top-k` | `-n` | Number of videos to process | `--top-k 10` |
| `--concurrency` | `-c` | Concurrent tasks (1-10) | `--concurrency 5` |
| `--pure-youtube` | | Fast mode: YouTube only, no AI | `--pure-youtube` |
| `--audio` | | Enable audio analysis | `--audio` |
| `--no-persist` | | Disable database storage | `--no-persist` |
| `--dry-run` | | Test mode: no API calls | `--dry-run` |
| `--config-test` | | Validate configuration | `--config-test` |
| `--verbose` | `-v` | Debug logging | `--verbose` |
| `--quiet` | `-q` | Minimal output | `--quiet` |
| `--log-file` | | Custom log file path | `--log-file debug.log` |


## 6. Testing
We have integrated `pytest` for unit testing. To ensure the test, what you can do is in project root run following
```bash
pytest --cov
```

## 7. Contributing
We welcome contributions from the open-source community. Here’s how you can contribute:

### Reporting Bugs[🪲]:
If you encounter any issues while using TubeWhale, please open an issue on GitHub with following:
- a clear description of the bug and steps to reproduce it.
- The way you think which module goes wrong. Any traceback?

### Pull Requests:
Fork the repository and create a new branch for your feature [🚩] or bugfix [🪲🔫] .

Commit your changes with clear and descriptive messages.
Push your branch to your forked repository.
Open a pull request describing the changes made. I will review when if I have the time 👀

## 8. Donation Polygon & Support 💖☕️

###  😊 I will apprecatie if you show your love or just buy me a cup of coffee ☕️.  
![Polygon](image.png)


# 9. License 📜
This project is licensed under the [MIT](https://mit-license.org/) License.

# 10. Contact 📧
For any inquiries or support, please contact: admin@jl-blog.com
Please include the header: [TubeWhale] Support/Question: ... in your email.

# 11. Citing TubeWhale🔖
If you use TubeWhale in your research or data collection, please consider citing our project to acknowledge our efforts. Proper citation supports the ongoing development of open-source tools.

## 12. Industrial CLI Execution Persistence & Knowledge Graph Exports

To support auditability, reproducibility, and downstream knowledge graph construction, TubeWhale now provides an industrial-grade persistence and export interface for every backend-triggered CLI execution.

### 12.1 Data Model
Each run is stored in `templates_cli_execution` with:
- `command`, `args`
- `template_id`, `expert_slug` (prompt context lineage)
- `prompt_final` (composed prompt after expert layering)
- `stdout`, `stderr`, `status`, `returncode`, `duration_ms`
- `meta` (JSON: includes user id or future tracing metadata)
- Timestamps

Statuses: `success`, `error`, `timeout`, `missing_entry`.

### 12.2 Admin UI
`CLIExecution` is read-only in Django Admin with filters (status, command, date). This provides a quick operational console for recent runs and debugging.

### 12.3 Programmatic Staff Endpoints
All endpoints are staff-only (session-auth protected):

1. Compose + Run (existing):
   `POST /admin/templates/compose-run/`
   Form fields: `template_id`, `expert_slug` (optional), `command`, `var_<name>` dynamic template variables.
   Returns: `prompt_final`, `cli` (stdout/stderr/returncode), `execution_id`.

2. Export Runs:
   `GET /admin/templates/cli-runs/export/<fmt>/?limit=1000&status=success&command=analyze_videos&full=1`
   Formats:
   - `json`: `{ ok, count, results:[...] }`
   - `ndjson`: newline-delimited JSON (stream/ingest friendly)
   - `csv`: tabular (stdout / stderr / prompt optionally included with `full=1`)

3. Graph Projection:
   `GET /admin/templates/cli-runs/graph/?limit=500&command=analyze_videos`
   Returns lightweight knowledge graph JSON:
   - `nodes`: templates (`type=template`), experts (`type=expert`), executions (`type=execution`)
   - `edges`: `template -> execution (USED_IN)`, `expert -> execution (INFLUENCES)`
   - `counts`: summary counts

### 12.4 Example NDJSON Ingestion Flow
```bash
curl -s -b sessionid=... 'https://yourhost/admin/templates/cli-runs/export/ndjson/?limit=2000' > cli_runs.ndjson
# Ingest into graph / analytics store
cat cli_runs.ndjson | jq -c 'select(.status=="success")' | some_pipeline_loader
```

### 12.5 Configuration
Persistence can be toggled via `CLI_PERSIST_ENABLED` (default True). When disabled, runtime performance is maximized (no DB writes) and export endpoints will simply return empty sets.

### 12.6 Extending the Graph
You can enrich the `meta` field during execution (e.g., correlation ids, cost metrics) and later surface additional edge types (e.g., `EXECUTES_AGENT`, `GENERATES_RESOURCE`). The current schema is intentionally minimal yet evolution-friendly.

### 12.7 Safety & Performance Notes
- Large stdout/stderr are truncated to 64KB each to prevent oversized rows.
- CSV export omits prompt/output unless `full=1` for better default performance.
- Indexes on (`command`, `created_at`), (`status`, `created_at`), (`template_id`, `expert_slug`) accelerate filtering & lineage queries.

### 12.8 Future Enhancements (Suggested)
- Async execution queue (Celery) with status updates.
- Signed export tokens for automation without full admin session.
- Incremental export via `?since=<ISO8601>`.
- Native Neo4j / RDF emitter for richer knowledge graph semantics.

## 13. Supervision & Operational Observability

Staff‑only (session authenticated) operational endpoints:

| Endpoint | Purpose |
|----------|---------|
| `/admin/templates/cli-runs/overview/` | Aggregated latency, error & timeout metrics + blockage heuristic |
| `/admin/templates/cli-runs/recent/` | Rolling recent execution list |
| `/admin/templates/cli-runs/export/<fmt>/` | Bulk export (json / ndjson / csv) with filters & optional `since` |
| `/admin/templates/cli-runs/graph/` | Lightweight lineage graph (templates / experts / executions) |
| `/admin/templates/compose-preview/` | Safe prompt layering preview (no execution) |
| `/admin/templates/compose-run/` | Compose + run synchronously |
| `/admin/templates/cli-runs/async/run/` | Enqueue async Celery task |
| `/admin/templates/cli-runs/async/status/<task_id>/` | Poll task + execution state |

### 13.1 Metrics & Health
`overview` computes p50/p95/p99 latencies, error & timeout rates, and a blockage flag if no recent successes while failures dominate.

### 13.2 Prompt Layering
`ExpertPrompt` wraps a base template with intro/outro segments to produce a composed `prompt_final`. Preview without side effects via `/compose-preview/`.

## 14. Asynchronous Execution

Celery integration (Redis broker) enables long‑running CLI tasks to execute off‑request:
1. POST async run → queued `CLIExecution` row (status=queued) + `task_id`.
2. Worker runs whitelisted command with timeout & truncation.
3. Status polling endpoint returns final stdout/stderr metadata.

Disable persistence: set `CLI_PERSIST_ENABLED=false` for maximum throughput (export endpoints return empty sets).

## 15. Deployment Stack

Included `deploy/docker-compose.yml` services:

| Service | Role |
|---------|------|
| web | Django application |
| worker | Celery worker |
| beat | Optional scheduled tasks |
| redis | Broker/cache |
| nginx | Reverse proxy & static/media |

### 15.1 Quick Start
```bash
docker compose -f deploy/docker-compose.yml up --build -d
docker compose -f deploy/docker-compose.yml exec web python manage.py migrate
docker compose -f deploy/docker-compose.yml exec web python manage.py createsuperuser
```

### 15.2 Key Environment Flags
| Variable | Purpose | Default |
|----------|---------|---------|
| `CLI_PERSIST_ENABLED` | Toggle execution persistence | true |
| `MAX_CLI_EXPORT_LIMIT` | Export row cap | 2000 |
| `PUBLIC_TEMPLATE_PREVIEW_ENABLED` | Public read‑only template index | false |
| `ENTERPRISE_TEMPLATE_DASHBOARD_ENABLED` | Enhanced dashboards | true |

### 15.3 Hardening Checklist
- Enforce HTTPS (Nginx + certbot)
- Set Django `SECURE_*` + `CSRF_COOKIE_SECURE`, `SESSION_COOKIE_SECURE`
- Use external DB if high write concurrency emerges
- Rotate OpenAI/YouTube keys via environment, never bake into image

### 15.4 Image Slimming
`.dockerignore` excludes large transient artifacts (`downloads/`, `logs/`, exports) to reduce context & layers.

## 16. Minimal Footprint Principle
- Remove dead/duplicate admin UIs instead of hiding them
- Guard optional modules (e.g., TemplateEngine) for graceful degradation
- Favor small, composable endpoints over monolith dashboards

## 17. Focused Roadmap
- Signed export tokens
- Streaming partial logs
- Cost/token accounting per execution
- Enhanced graph export (Neo4j / RDF)
- Stable incremental cursor (`since`) watermarking

## 18. CLI External DB Introspection (Admin)

When the standalone CLI pipeline writes to its own SQLite file (configured via `DB_PATH` in `.env`), staff users can now introspect it safely (read‑only) from the Django Admin without merging schemas. This enables cross‑analysis and faster debugging while preserving isolation.

### 18.1 Features
| Endpoint | Description |
|----------|-------------|
| `/admin/templates/cli-db/` | Overview: list tables + row counts + export links |
| `/admin/templates/cli-db/preview/<table>/` | Paginated preview (limit/offset) + schema PRAGMA |
| `/admin/templates/cli-db/export/<table>/` | Export a single table in `json`, `ndjson`, or `csv` |

### 18.2 Safety Guards
* Read‑only: only `SELECT` and `PRAGMA` are executed.
* Hard caps: preview ≤ 200 rows; export ≤ 10,000 rows.
* Graceful failure: missing DB path returns empty overview with guidance.
* Path resolution: relative `DB_PATH` is resolved against project base; nonexistent file ignored.

### 18.3 Usage Examples
Download a table as NDJSON:
```bash
curl -s -b sessionid=... \
   'https://yourhost/admin/templates/cli-db/export/ai_interactions/?fmt=ndjson&limit=500' > ai_interactions.ndjson
```

Preview with custom limit:
```
https://yourhost/admin/templates/cli-db/preview/keyword_analysis/?limit=100&offset=0
```

### 18.4 Extensibility
Future small additions could include:
* Full‑text search across selected text columns.
* Inline JSON expansion (pretty view) for nested blobs.
* Sampling strategies (e.g., `ORDER BY RANDOM()` for large tables).

## 19. Expert Prompt Seed System

The platform seeds three core expert prompts (bilingual) that extract structured insights from YouTube transcripts:

Slugs:
- `expert_core_insights` – Core topics, problems, value proposition, unique value elements, evidence quotes.
- `expert_action_playbook` – Actionable tactics (categorized), frameworks/models, tool recommendations, common mistakes.
- `expert_risk_and_improvements` – Risks (typed with mitigation), assumptions, limitations, improvement opportunities, missing perspectives.

Implementation:
- Data migration `0005_seed_expert_prompts` inserts them idempotently.
- Management command: `python manage.py seed_expert_prompts --force` refreshes content.
- Admin overview: `/admin/templates/expert-prompts/` (staff only) lists prompt metadata.

Extension Guide:
1. Add new prompt entry via Django admin or create a new migration with a pattern similar to `0005`.
2. Use `weight` to control ordering (lower = earlier injection precedence).
3. Use `prompt_intro` and `prompt_outro` to wrap any base template text. The system composes: intro + base + outro.
4. Keep output schemas stable (prefer additive changes over destructive) so downstream parsers remain compatible.

Safety / Anti-Hallucination Measures:
- Each prompt explicitly instructs grounding in transcript text.
- JSON-only output enforced in `prompt_outro` to make parsing deterministic.

Future Roadmap Ideas:
- Add competitive analysis prompt.
- Add KPI extraction & metric suggestion prompt.
- Domain-specific overrides (e.g., medical vs. policy) using `domain` + `role` combos.
If you migrate the CLI to a shared RDBMS (PostgreSQL), these endpoints continue to operate on the separate SQLite file unless you align `DB_PATH` to the unified store.




## 20. Tier-Based Template Access & Selection (BASIC / PRO / ENTERPRISE)

Granular access to built‑in templates is enforced by a selection + tier model:

Tiers:

### 20.1 Data Model
`UserTemplateSelection` persists BASIC user locked choices:

### 20.2 Selection API (Authenticated)
| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/templates/selection/` | List active selections + tier stats |
| POST | `/templates/selection/add/` | Add a selection (BASIC only enforced) |
| DELETE | `/templates/selection/remove/<template_id>/` | Deactivate a selection |

POST body fields:
```json
{
   "template_id": "core_summary_v1",
   "template_name": "Summary V1",   // optional (defaults to template_id)
   "template_type": "core"           // core | domain (default core)
}
```
Errors:

### 20.3 Access Logic
Implemented in `templates_app.access_control` and updated `tier_utils`:

## 22. Job API (Non-Technical Wrapper)

To simplify execution for non-technical users, a high-level Job abstraction wraps CLI executions:

Endpoint summary (base: `/api/templates/`):
- `POST /jobs/` Create a job. Body: `{ "command": "health_check", "template_id": "optional", "expert_slug": "optional", "variables": {}}`
- `GET /jobs/` List user jobs (most recent first)
- `GET /jobs/{id}/` Retrieve single job
- `POST /jobs/{id}/cancel/` Attempt cancellation (best-effort Celery revoke)

States:
`pending -> queued -> running -> success | error | canceled`

Fields:
- `execution_id`: Links to underlying `CLIExecution` record (if persisted) for stdout/stderr auditing.
- `variables`: Optional dict used for prompt variable interpolation when `template_id` supplied.
- `progress`: Currently coarse (0 or 100) but reserved for future granular updates.

Degraded Mode:
If Celery broker is unavailable, job executes inline synchronously and status will jump directly to `success` or `error`.

Cancellation:
Attempts Celery revoke; if already finished returns validation message. Inline/degraded jobs may finish before cancel request.

Admin:
`Job` model is registered in Django admin for operational monitoring.

Testing:
See `tests/test_job_api.py` for basic lifecycle tests.

Roadmap Enhancements:
- Progress callbacks (stream token counts, step phases)
- Aggregated cost/token reporting per job
- Retry / duplicate submission idempotency tokens
- User notification hooks (websocket / email)

### 23.1 Docker FFmpeg Support
## 24. Admin Dashboard & Monitoring

The Django Admin homepage is overridden to provide an at-a-glance operational dashboard:

Features:
- Quick links: System Health, Jobs, CLI Executions, Template Selections, Custom Templates, Expert Prompts.
- Snapshot panels: totals (Jobs, Executions, Active Users 7d).
- Recent Jobs & Executions tables (latest 5).
- Failed Jobs list (latest 5 errors, truncated).
- Dedicated System Health page (runtime metrics: CPU/Memory/Disk, Celery presence, DB migration drift, template selection stats, beat table existence, ffmpeg availability via healthcheck).

URLs:
- `/admin/` main dashboard
- `/admin/system-health/` detailed system health

System Metrics:
- `psutil>=5.9,<6.0` is a required dependency for runtime CPU/memory/disk monitoring - application enforces this at startup.

Security / Operational Notes:
- Auto superuser provisioning (admin/admin123) occurs only if no admin user exists; change password post-deploy.
- For production, set `DJANGO_SUPERUSER_PASSWORD` and remove default user creation logic if desired.
- To disable inline metrics for compliance, simply remove `templates/admin/index.html` and the view entries from `urls.py`.

Roadmap Enhancements (not yet implemented):
- Redis queue depth / Celery active worker counts.
- Token/cost aggregation per Job.
- WebSocket push for near real-time status.
- Exportable JSON monitoring endpoint for external dashboards.
The container image now bundles `ffmpeg` (installed via apt) and the healthcheck validates both API readiness and ffmpeg availability. If you previously saw a runtime warning like:
```
RuntimeWarning: Couldn't find ffmpeg or avconv - defaulting to ffmpeg, but may not work
```
Rebuild the image to resolve:
```
docker compose build backend
docker compose up -d
```
You can verify inside the container:
```
docker compose exec backend ffmpeg -version
```

## 23. Job Logs Endpoint & Dev Environment

Logs Endpoint:
`GET /api/templates/jobs/{id}/logs/` returns (when ready):
```json
{
   "job_id": 7,
   "execution_id": 42,
   "command": "health_check",
   "status": "success",
   "cli_status": "success",
   "stdout": "...truncated...",
   "stderr": "",
   "returncode": 0,
   "duration_ms": 1532
}
```
Responses:
- `202` with `{ "detail": "execution_pending" }` if execution not yet linked.
- `404` if job or execution missing.

Dev Requirements:
Install development/testing dependencies:
```
pip install -r requirements-dev.txt
```
Run tests (example subset):
```
pytest -k job -q
```
Coverage run:
```
coverage run -m pytest && coverage report -m
```
- BASIC: Only templates present in `UserTemplateSelection(active=True)` are returned by filtering helpers.
- PRO / ENTERPRISE: All built‑in templates surfaced; ENTERPRISE also gains future customization privileges.
- Staff: Bypass restrictions.

### 20.4 Downgrade / Reconciliation
Signal: On `UserProfile` save, if tier is BASIC the system prunes oldest selections beyond the limit (currently hard‑coded `BASIC_LIMIT = 3`). Utility: `reconcile_user_selections(user)` may be called manually (future admin button candidate).

### 20.5 Admin Observability
Staff view: `/admin/templates/user-template-selections/` (template: `admin/user_template_selections_overview.html`) lists per‑user counts, template ids, most recent lock timestamp. BASIC users exceeding 3 would appear (should not after prune, but view helps audit).

### 20.6 Stats Endpoint Semantics
`GET /templates/selection/` response:
```json
{
   "tier": "BASIC",
   "max": 3,
   "selected": [ {"template_id": "...", "template_name": "...", "template_type": "core", "locked_at": "..."} ],
   "remaining": 1
}
```
For PRO/ENTERPRISE: `max`, `remaining` may be `null` (unbounded access).

### 20.7 Testing Coverage
`tests/test_template_selection_tiers.py` validates:
- BASIC user can select up to 3; 4th attempt fails.
- PRO user sees unrestricted builtin list (no selections required).
- Downgrade from PRO → BASIC prunes to limit.

### 20.8 Extensibility Roadmap
- Per‑domain caps (e.g., 2 core + 1 domain) with dynamic UI hints.
- Soft‑lock countdown (trial rotation) allowing periodic template swaps.
- Selection history audit trail (activation/deactivation logs).
- Admin forced reallocation (e.g., expire stale selections automatically after inactivity).
- Dynamic limit sourced from billing plan metadata instead of hard constant.

### 20.9 Migration & Backward Compatibility
Legacy usage counting (`TemplateUsage`) remains for analytics but is no longer used for BASIC gating; gating now depends solely on explicit selection records. This ensures deterministic template sets and prevents accidental quota leakage through one‑off usage spikes.

## 21. Industrial Ops & System Health

To support a more “工业级 / AA 级”运营体验，已引入一组后端可观测与自动化增强：

### 21.1 System Health Admin 页面
路径：`/admin/templates/system/health/`

展示内容：
- 当前数据库引擎 / 名称
- 未应用迁移数量与列表（pending migrations）
- Celery Beat 关键调度表存在性检测
- 用户模板选择（BASIC）总量、涉及用户数、超限用户（理论上会被自动裁剪）

该页面帮助快速判断：是否部署到正确的 Postgres、迁移是否遗漏、Beat 是否初始化成功、BASIC 选择机制是否有异常。

### 21.2 Celery Beat 基线任务播种
管理命令：
```bash
python manage.py seed_periodic_tasks
```
可选 `--force` 重新覆盖：
```bash
python manage.py seed_periodic_tasks --force
```
默认创建任务：`Cleanup Old Video Analysis Tasks`（后续可扩展更多周期任务）。

### 21.3 环境变量一体化 (.env)
为避免 Worker / Beat fallback 到 sqlite（触发 “no such table: django_celery_beat_periodictask”），`.env` 需包含：
```
DJANGO_SETTINGS_MODULE=tubewhale_project.settings
DB_ENGINE=django.db.backends.postgresql
DB_NAME=tubewhale
DB_USER=tubewhale
DB_PASSWORD=tubewhale123
DB_HOST=postgres
DB_PORT=5432
REDIS_URL=redis://:redis123@redis:6379/0
CELERY_BROKER_URL=redis://:redis123@redis:6379/0
CELERY_RESULT_BACKEND=redis://:redis123@redis:6379/0
```

### 21.4 常见运维故障指纹
| 症状 | 可能原因 | 解决 | 
|------|----------|------|
| Beat 循环重启 + sqlite 表不存在 | `.env` 未注入 DB_* | 添加 DB_* 变量 + 重建容器 |
| Worker unhealthy | 长时间阻塞或探针策略不匹配 | 查看 `docker compose logs celery_worker` 优化超时或探针 |
| 模板 BASIC 选择混乱 | Tier 变更未裁剪 | 触发 `UserProfile` 保存或调用 reconcile 工具 |

### 21.5 后续可演进方向
- 增加 Prometheus 指标端点 (celery 队列深度 / 模板访问统计)
- 增加管理命令：`ops_diagnose` 输出加密脱敏 JSON 诊断包
- 增加 WebSocket 异步任务实时进度面板
- 自动化“冷启动自检”脚本 (在容器启动后跑一遍模型装载 & 迁移 & beat 表核验)





