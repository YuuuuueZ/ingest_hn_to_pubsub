import os, json, requests
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from google.cloud import bigquery
import functions_framework

PROJECT_ID = "reddit-intelligence-platform"
TOPIC_ID = "reddit-stream-topic"
HN_BASE = "https://hacker-news.firebaseio.com/v0"

# BigQuery state table config
BQ_DATASET = os.environ.get("BQ_DATASET", "hn_analytics")
STATE_TABLE = os.environ.get("STATE_TABLE", "backfill_state")
STATE_NAME = os.environ.get("STATE_NAME", "hn_3mo")

publisher = pubsub_v1.PublisherClient(transport="rest")
bq = bigquery.Client()

def fetch_json(url):
    return requests.get(url, timeout=5).json()

def hn_item_to_record(item):
    ts = None
    if item.get("time"):
        ts = datetime.fromtimestamp(item["time"], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "id": str(item.get("id")),
        "type": item.get("type"),
        "title": item.get("title"),
        "text": item.get("text"),
        "by": item.get("by"),
        "time": ts,
        "score": int(item.get("score", 0) or 0),
        "descendants": int(item.get("descendants", 0) or 0),
        "url": item.get("url"),
    }

def publish(topic_path, record):
    data = json.dumps(record).encode("utf-8")
    publisher.publish(topic_path, data=data)  # 不要 .result()，避免卡住

from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_item(_id):
    try:
        item = fetch_json(f"{HN_BASE}/item/{_id}.json")
        return _id, item
    except Exception:
        return _id, None

def backfill_range(topic_path, start_id, end_id, limit):
    end = min(end_id, start_id + limit - 1)
    ids = list(range(start_id, end + 1))

    scanned = len(ids)
    published = 0

    max_workers = int(os.environ.get("MAX_WORKERS", "20"))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_item, _id) for _id in ids]

        for future in as_completed(futures):
            _id, item = future.result()
            if not item:
                continue
            if item.get("type") != "story":
                continue
            record = hn_item_to_record(item)
            publish(topic_path, record)
            published += 1

    return scanned, published, end

def read_state():
    sql = f"""
    SELECT next_start_id, end_id
    FROM `{PROJECT_ID}.{BQ_DATASET}.{STATE_TABLE}`
    WHERE name = @name
    LIMIT 1
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("name", "STRING", STATE_NAME)]
        ),
    )
    rows = list(job.result())
    if not rows:
        raise RuntimeError(f"State row not found: {PROJECT_ID}.{BQ_DATASET}.{STATE_TABLE} name={STATE_NAME}")
    return int(rows[0]["next_start_id"]), int(rows[0]["end_id"])

def update_state(next_start_id):
    sql = f"""
    UPDATE `{PROJECT_ID}.{BQ_DATASET}.{STATE_TABLE}`
    SET next_start_id = @next_start_id,
        updated_at = CURRENT_TIMESTAMP()
    WHERE name = @name
    """
    bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("next_start_id", "INT64", int(next_start_id)),
                bigquery.ScalarQueryParameter("name", "STRING", STATE_NAME),
            ]
        ),
    ).result()

@functions_framework.http
def ingest(request):
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    args = request.args or {}
    mode = args.get("mode", "stream")

    if mode == "backfill_auto":
        limit = int(args.get("limit", "50"))  # 先小一点稳
        next_start_id, end_id = read_state()

        if next_start_id > end_id:
            return (json.dumps({
                "ok": True,
                "mode": "backfill_auto",
                "done": True,
                "next_start_id": next_start_id,
                "end_id": end_id
            }), 200, {"Content-Type": "application/json"})

        scanned, published, end_reached = backfill_range(topic_path, next_start_id, end_id, limit)
        update_state(end_reached + 1)

        return (json.dumps({
            "ok": True,
            "mode": "backfill_auto",
            "done": False,
            "start_id": next_start_id,
            "end_id": end_id,
            "end_reached": end_reached,
            "scanned": scanned,
            "published": published,
            "next_start_id_after": end_reached + 1
        }), 200, {"Content-Type": "application/json"})

    # 你原来的手动 backfill 还可以保留（可选）
    if mode == "backfill":
        start_id = int(args.get("start_id", "0"))
        end_id = int(args.get("end_id", "0"))
        limit = int(args.get("limit", "50"))
        if start_id <= 0 or end_id <= 0 or start_id > end_id:
            return (json.dumps({"ok": False, "error": "need valid start_id/end_id"}), 400)
        scanned, published, end_reached = backfill_range(topic_path, start_id, end_id, limit)
        return (json.dumps({
            "ok": True,
            "mode": "backfill",
            "start_id": start_id,
            "end_id": end_id,
            "end_reached": end_reached,
            "scanned": scanned,
            "published": published
        }), 200, {"Content-Type": "application/json"})

    # 默认 stream：每次请求抓 newstories
    n = int(args.get("n", "30"))
    ids = fetch_json(f"{HN_BASE}/newstories.json")[:n]
    published = 0
    for _id in ids:
        item = fetch_json(f"{HN_BASE}/item/{_id}.json")
        if not item:
            continue
        record = hn_item_to_record(item)
        publish(topic_path, record)
        published += 1

    return (json.dumps({"ok": True, "mode": "stream", "published": published}), 200, {"Content-Type": "application/json"})

