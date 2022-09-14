from datetime import datetime
from io import StringIO
import json
import os
import sys
from time import sleep

import requests

# {\"event_name\": \"metaflow_flow_run_succeeded\", \"timestamp\": 1663184739,
# \"event_id\": \"123456789123\", \"flow_name\": \"HelloFlow\", \"run_id\": \"argo-helloflow-atykf\"}


def build_route(md_url, flow_name=None, run_id=None, step_name=None, task_id=None):
    buf = StringIO(md_url)
    buf.write("/flows")
    if flow_name is not None:
        buf.write(f"/{flow_name}")
        if run_id is not None:
            buf.write(f"/runs/{run_id}")
            if step_name is not None:
                buf.write(f"/steps/{step_name}")
                if task_id is not None:
                    buf.write(f"/tasks/{task_id}")
    return buf.getvalue()


def build_metadata():
    arg_count = len(sys.argv)
    if arg_count < 6:
        raise RuntimeError(f"Expected at least 5 arguments. Received {arg_count - 1}.")
    if (arg_count - 1) % 5 != 0:
        raise RuntimeError(
            f"Expected args to be a multiple of 5. Received {arg_count - 1}."
        )

    md = []
    event_offset = 1
    while event_offset < arg_count:
        event = {
            "event_name": sys.argv[event_offset],
            "timestamp": int(sys.argv[event_offset + 1]),
            "event_id": sys.argv[event_offset + 2],
            "flow_name": sys.argv[event_offset + 3],
            "run_id": sys.argv[event_offset + 4],
        }
        md.append(event)
        event_offset += 5
    return md


def get_start_task_id(url, headers):
    start_ts = datetime.utcnow()
    headers.update({"accept": "application/json"})
    # Try for 1 minute
    while (datetime.utcnow() - start_ts).seconds < 60:
        try:
            response = requests.get(url, headers=headers, timeout=1)
        except requests.ReadTimeout:
            continue
        if response.status_code == 404:
            sleep(1)
            continue
        elif response.status_code == 200:
            body = response.json()
            if len(body) == 0:
                sleep(1)
                continue
            else:
                task = body[0]
                return task["task_id"]
        else:
            raise RuntimeError(
                f"Expecting 404 or 200 response. Received {response.status_code}"
            )
    raise RuntimeError("Exceeded 60 seconds to retrieve start step metadata")


def send_metadata(md_url, headers):
    md = build_metadata()
    user = os.getenv("METAFLOW_USER")
    flow_name = os.getenv("METAFLOW_FLOW_NAME")
    run_id = os.getenv("METAFLOW_RUN_ID")
    step_url = build_route(
        md_url, flow_name=flow_name, run_id=run_id, step_name="start"
    )
    task_id = get_start_task_id(step_url, headers)
    task_url = build_route(
        md_url, flow_name=flow_name, run_id=run_id, step_name="start", task_id=task_id
    )
    headers.update({"content-type": "application/json"})
    body = {
        "field_name": "event_trigger",
        "value": json.dumps(md, separators=(",", ":"), indent=None),
        "type": "event_trigger",
        "user": user,
    }
    response = requests.post(url=task_url, headers=headers, json=body)
    response.raise_for_status()


def main():
    text_headers = os.getenv("METAFLOW_SERVICE_HEADERS")
    if text_headers is None:
        raise RuntimeError("Required env var METAFLOW_SERVICE_HEADERS is missing")
    headers = json.loads(text_headers)
    md_url = os.getenv("METAFLOW_SERVICE_URL")
    if md_url is None:
        raise RuntimeError("Required env var METAFLOW_SERVICE_URL is missing")
    if md_url.startswith("http://") or md_url.startswith("https://"):
        send_metadata(md_url, headers)
    else:
        raise RuntimeError(
            f"Unknown metadata service URL. Expecting either http or https: {md_url}"
        )


if __name__ == "__main__":
    main()
