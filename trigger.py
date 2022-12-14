import asyncio
import base64
from datetime import datetime
import json
import os
import sys
from time import sleep

HTTP_PROTOCOLS = ["http://", "https://"]

result = 0


def call_http(url, body):
    import requests

    resp = requests.post(url, headers={"content-type": "application/json"}, json=body)
    if resp.status_code >= 400:
        result = 1


async def call_nats(event_source, msg):
    import nats

    auth_token = os.getenv("NATS_TOKEN")
    chunks = event_source.split("/")
    topic = chunks[-1]
    nats_host = f"nats://{chunks[2]}"
    try:
        conn = await nats.connect(nats_host, token=auth_token)
        body = bytes(json.dumps(msg), "utf-8")
        await conn.publish(topic, body)
        await conn.drain()
    except Exception as e:
        print(f"NATS Error: {e}")
        result = 1


def decode_user_data(data):
    raw = bytes(data, "UTF-8")
    decoded = base64.b64decode(raw)
    text = decoded.decode(encoding="UTF-8")
    return json.loads(text)


def make_user_event_payload(args, data):
    ts = int(datetime.utcnow().timestamp())
    data["metaflow_trigger_timestamp"] = ts
    if len(args) < 1:
        raise RuntimeError("Expected at least 1 arg for user events; have 0")
    event_name = args[0]
    user_data = {}
    if len(args) > 1 and args[1] is not None:
        user_data = decode_user_data(args[1])
    for k in user_data.keys():
        if k not in data.keys():
            data[k] = user_data[k]
    return {"payload": {"event_name": event_name, "timestamp": ts, "data": data}}


def make_lifecycle_event_payload(args, data):
    ts = int(datetime.utcnow().timestamp())
    if len(args) < 2:
        raise RuntimeError(
            f"Expected at least 2 args for lifecycle events; have {len(args)}"
        )
    flow_name = args[0]
    flow_status = args[1]
    event_name = f"metaflow_flow_run_{flow_status}"
    user_data = {}
    if len(args) > 2 and args[2] is not None:
        user_data = decode_user_data(args[2])
    for k in user_data.keys():
        if k not in data.keys():
            data[k] = user_data[k]
    return {
        "payload": {
            "event_name": event_name,
            "flow_name": flow_name,
            "timestamp": ts,
            "data": data,
        }
    }


def main():
    runtime = os.getenv("METAFLOW_RUNTIME_NAME")
    if runtime != "argo-workflows":
        raise RuntimeError(f"Unknown runtime: {runtime}")
    event_source = os.getenv("METAFLOW_EVENT_SOURCE")
    if event_source is None:
        raise RuntimeError("METAFLOW_EVENT_SOURCE not set")

    if len(sys.argv) < 2:
        raise RuntimeError(f"Not enough arguments: {len(sys.argv)}")

    args = sys.argv[2:]
    if sys.argv[1] == "user_event":
        payload = make_user_event_payload(args, {})
    else:
        payload = make_lifecycle_event_payload(args, {})
    print(f"payload: {payload}")
    if event_source in HTTP_PROTOCOLS:
        call_http(event_source, payload)
    else:
        asyncio.run(call_nats(event_source, payload))
    return result


if __name__ == "__main__":
    result = main()
    print(f"Event trigger returned: {result}")
    sleep(1)
