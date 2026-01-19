import json
import time
import requests
from datetime import datetime, timezone
from google.cloud import pubsub_v1

# ===== 配置 =====
PROJECT_ID = "reddit-intelligence-platform"
TOPIC_ID = "reddit-stream-topic"
HN_BASE = "https://hacker-news.firebaseio.com/v0"
POLL_SECONDS = 20

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

publisher.publish(topic_path, b'{"test": "hello pubsub"}')
print("Published test message")

seen_ids = set()

def fetch_json(url):
    return requests.get(url, timeout=10).json()

def hn_item_to_record(item):
    return {
        "id": f"hn_{item.get('id')}",
        "subreddit": "hackernews",
        "title": item.get("title", ""),
        "body": item.get("text", ""),
        "author": item.get("by"),
        "timestamp": datetime.fromtimestamp(
            item.get("time"), tz=timezone.utc
        ).isoformat() if item.get("time") else None,
        "score": item.get("score", 0),
        "kind": item.get("type", "story"),
        "url": item.get("url")
    }

def publish(record):
    data = json.dumps(record).encode("utf-8")
    publisher.publish(topic_path, data=data)

def main():
    print(f"Publishing Hacker News data to {topic_path}")
    while True:
        try:
            ids = fetch_json(f"{HN_BASE}/newstories.json")[:30]
            for _id in ids:
                if _id in seen_ids:
                    continue
                item = fetch_json(f"{HN_BASE}/item/{_id}.json")
                if not item:
                    continue
                record = hn_item_to_record(item)
                publish(record)
                seen_ids.add(_id)
                print("Published", record["id"])
            time.sleep(POLL_SECONDS)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print("Error:", e)
            time.sleep(5)



if __name__ == "__main__":
    main()