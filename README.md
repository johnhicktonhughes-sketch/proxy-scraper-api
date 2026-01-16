# Proxy Scraper API

Simple FastAPI CRUD service for the `scrape_tasks` table in Postgres.

## Setup

1) Create and activate a virtualenv.
2) Install dependencies:

```bash
pip install -r requirements.txt
```

3) Create a `.env` file:

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/dbname
API_KEY=change-me
```

## Run

```bash
uvicorn app.main:app --reload
```

All endpoints require `X-API-Key`:

```bash
curl -H "X-API-Key: change-me" http://127.0.0.1:8000/scrape_tasks
```

## Endpoints

- `GET /scrape_tasks` with optional filters and range params:
  - `site`, `task_type`, `status`
  - `scheduled_at`, `scheduled_at_from`, `scheduled_at_to`
  - `created_at`, `created_at_from`, `created_at_to`
- `GET /scrape_tasks/{task_id}`
- `POST /scrape_tasks`
- `PATCH /scrape_tasks/{task_id}`
- `DELETE /scrape_tasks/{task_id}` (only pending/failed)
- `GET /scrape_tasks/enums`

## Response shape

`GET /scrape_tasks` returns:

```json
{
  "total": 0,
  "items": []
}
```

## Deploy (Railway)

1) Create a new Railway project from this repo.
2) Add environment variables:
   - `DATABASE_URL` (Railway Postgres connection string)
   - `API_KEY`
3) Set the start command:

```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```
