import random
import json

HOST = "http://host.docker.internal:8080"
REQUEST_COUNT = 6000

with open("get_targets.jsonl", "w") as f:
    for _ in range(REQUEST_COUNT):
        entity_id = random.randint(1, 1_000_000)

        target = {
            "method": "GET",
            "url": f"{HOST}/v0/entity/{entity_id}",
            "header": {
                "Content-Type": ["application/json"]
            }
        }

        f.write(json.dumps(target) + "\n")