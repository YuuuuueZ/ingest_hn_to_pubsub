import os
import json
import requests
from datetime import datetime, timezone
from google.cloud import pubsub_v1
import functions_framework

# ===== 配置（用环境变量覆盖）=====
PROJECT_ID = "reddit-intelligence-platform"
TOPIC_ID = "reddit-stream-topic"
HN_BASE = "https://hacker-news.firebaseio.com/v0"
N_STORIES = int(os.environ.get("N_STORIES", "30"))

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
    # 等待返回 message id，方便确认真的发出去了
    return publisher.publish(topic_path, data=data).result()

@functions_framework.http
def ingest(request):
    # 支持 GET/POST，都行
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    print(f"INGEST HIT. Publishing to: {topic_path}", flush=True)

    ids = fetch_json(f"{HN_BASE}/newstories.json")[:N_STORIES]

    published = 0
    for _id in ids:
        item = fetch_json(f"{HN_BASE}/item/{_id}.json")
        if not item:
            continue
        record = hn_item_to_record(item)
        msg_id = publish(topic_path, record)
        published += 1
        print(f"Published id={record['id']} msg_id={msg_id}", flush=True)

    return (json.dumps({"status": "ok", "published": published}), 200, {"Content-Type": "application/json"})

