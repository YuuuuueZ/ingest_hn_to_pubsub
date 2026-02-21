import os, json, requests
from datetime import datetime, timezone
from google.cloud import pubsub_v1
import functions_framework

PROJECT_ID = "reddit-intelligence-platform"
TOPIC_ID = "reddit-stream-topic"
HN_BASE = "https://hacker-news.firebaseio.com/v0"

DEFAULT_N_STORIES = int(os.environ.get("N_STORIES", "30"))
DEFAULT_LIMIT = int(os.environ.get("BACKFILL_LIMIT", "300"))

publisher = pubsub_v1.PublisherClient()

def fetch_json(url):
    return requests.get(url, timeout=10).json()

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
    return publisher.publish(topic_path, data=data).result()

def ingest_newstories(topic_path, n=30):
    ids = fetch_json(f"{HN_BASE}/newstories.json")[:n]
    published = 0
    for _id in ids:
        item = fetch_json(f"{HN_BASE}/item/{_id}.json")
        if not item:
            continue
        record = hn_item_to_record(item)
        publish(topic_path, record)
        published += 1
    return published

def backfill_range(topic_path, start_id, end_id, limit):
    # 限制本次请求处理数量，避免超时
    end = min(end_id, start_id + limit - 1)
    published = 0
    scanned = 0
    for _id in range(start_id, end + 1):
        scanned += 1
        item = fetch_json(f"{HN_BASE}/item/{_id}.json")
        if not item:
            continue
        # 只要 story/comment 之类你关心的类型（可选）
        if item.get("type") not in {"story"}:
            continue
        record = hn_item_to_record(item)
        publish(topic_path, record)
        published += 1
    return scanned, published, end

@functions_framework.http
def ingest(request):
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    mode = (request.args.get("mode") if request.args else None) or "stream"

    if mode == "backfill":
        start_id = int(request.args.get("start_id", "0"))
        end_id = int(request.args.get("end_id", "0"))
        limit = int(request.args.get("limit", str(DEFAULT_LIMIT)))
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

    # default: stream
    n = int(request.args.get("n", str(DEFAULT_N_STORIES))) if request.args else DEFAULT_N_STORIES
    published = ingest_newstories(topic_path, n=n)
    return (json.dumps({"ok": True, "mode": "stream", "published": published}), 200, {"Content-Type": "application/json"})

