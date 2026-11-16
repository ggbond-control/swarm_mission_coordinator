"""Generate self-contained local mock demo inputs; does not launch anything."""

import argparse
import json
from pathlib import Path

import yaml
from PIL import Image


def main():
    parser = argparse.ArgumentParser(
        description="Prepare a localhost-only mock swarm demo"
    )
    parser.add_argument("output", type=Path)
    parser.add_argument("--robots", type=int, choices=range(1, 6), default=3)
    parser.add_argument("--epoch", type=int, default=1)
    args = parser.parse_args()
    if args.epoch <= 0:
        parser.error("epoch must be positive")
    root = args.output.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    maps = root / "maps"
    maps.mkdir(exist_ok=True)
    Image.new("L", (120, 120), 255).save(maps / "map_000.pgm")
    (maps / "map_000.yaml").write_text(
        yaml.safe_dump(
            {
                "image": "map_000.pgm",
                "resolution": 0.5,
                "origin": [0, 0, 0],
                "free_thresh": 0.196,
                "occupied_thresh": 0.65,
            }
        )
    )
    robots = [f"SIM-{i + 1}" for i in range(args.robots)]
    tasks = [
        {
            "id": f"point-{i + 1}",
            "location": {"frame_id": "map_000", "pos": [3.0 + 8 * i, 4.0 + 8 * i, 0]},
            "tasks": [],
        }
        for i in range(args.robots)
    ]
    (root / "mission.json").write_text(
        json.dumps(
            {
                "round_epoch": args.epoch,
                "robot_ids": robots,
                "map_package": str(maps),
                "tasks": tasks,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    for robot in robots:
        config = {
            "robot_id": robot,
            "cluster_id": "local-demo",
            "data_root": str(root / "runtime"),
            "transport": {
                "mqtt_host": "127.0.0.1",
                "platform_prefix": "fh/swarm/dev/dispatch",
            },
            "agent": {"allow_motion": True, "task_hub_prefix": "/swarm/mock"},
            "planner": {"footprint_radius": 0.1},
            "coordinator": {"footprint_radius": 0.1},
        }
        (root / (robot + ".yaml")).write_text(yaml.safe_dump(config))
    print(str(root / "mission.json"))
    print(
        "Only mock TaskHub endpoints are configured. Use separate ROS domains per robot."
    )
