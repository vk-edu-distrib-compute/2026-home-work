import random
import base64
import json

HOST = "http://host.docker.internal:8080"
REQUEST_COUNT = 6000

with open("put_targets.jsonl", "w") as f:
    for _ in range(REQUEST_COUNT):
        entity_id = random.randint(1, 1_000_000)

        body = json.dumps({"value": f"data_{entity_id}"}).encode()
        body_b64 = base64.b64encode(body).decode()

        target = {
            "method": "PUT",
            "url": f"{HOST}/v0/entity/{entity_id}",
            "body": body_b64,
            "header": {
                "Content-Type": ["application/json"]
            }
        }

        f.write(json.dumps(target) + "\n")