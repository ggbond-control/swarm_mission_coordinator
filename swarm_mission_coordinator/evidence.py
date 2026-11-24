"""Export a verified round archive from the local swarm runtime."""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import tarfile
import time

from swarm_protocol.audit import redact
from swarm_protocol.contracts import canonical

SUFFIXES = {".log", ".json", ".jsonl", ".html", ".yaml", ".png", ".jpg", ".jpeg"}


def selected(root, output, epoch):
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or not path.is_file() or path.resolve() == output or path.suffix not in SUFFIXES:
            continue
        relative = path.relative_to(root)
        if any(part == "private" or part.startswith(("private_", ".")) or part == "credentials.json"
               for part in relative.parts):
            continue
        if relative.parts[0] in {"validation", "archives"} or path.name in {
            "raft.json", "execution.json", "outbox.json", "job.json", "migration.json"
        }:
            continue
        if epoch is not None and not (
            f"round-{epoch}" in relative.parts
            or path.name.startswith((f"sw{epoch}-", f"round-{epoch}-"))
            or (path.name == f"{epoch}.json" and relative.parent.name == "history")
            or relative.parts[0] == "ros_logs"
            or relative == Path("runtime-info.json")
            or (relative.parts[-2:] == ("audit", "status.json"))
            or (relative.parts[0] == "audit" and "round-0" in relative.parts)
            or (relative.parts[0] == "transport" and "round-0" in relative.parts)
        ):
            continue
        yield path, relative


def export(runtime, output, epoch, report_root=None, extra_roots=None):
    roots = {"runtime": Path(runtime)}
    if report_root is not None and Path(report_root).is_dir():
        roots["reports"] = Path(report_root)
    roots.update(extra_roots or {})
    report_root = roots.get("reports")
    camera_root = roots["runtime"] / "coordinator" / "media"
    camera_ids = set()
    cameras, files, missing, timestamps = [], [], [], []
    map_digests = set()
    if report_root and epoch is not None:
        marker = b'<script type="application/json" id="swarm-media-manifest">'
        for report in (report_root / f"round-{epoch}").glob("*.html"):
            try:
                raw = report.read_bytes()
                if marker in raw:
                    ids = json.loads(raw.split(marker, 1)[1].split(b"</script>", 1)[0])
                    if not isinstance(ids, list):
                        raise ValueError("invalid media manifest")
                    camera_ids.update(value for value in ids if isinstance(value, str))
            except (OSError, ValueError):
                missing.append(f"{report.name} 的图片清单不可读")
    for identity in camera_ids:
        if isinstance(identity, str) and len(identity) == 64 and all(c in "abcdef0123456789" for c in identity):
            cameras.extend(path for path in camera_root.glob(identity + ".*")
                           if path.is_file() and not path.is_symlink())
    output = Path(output).resolve()
    identity = roots["runtime"] / "runtime-info.json"
    info = {}
    try:
        if identity.is_file():
            info = redact(json.loads(identity.read_text()))
            if not isinstance(info, dict):
                raise ValueError("invalid runtime info")
    except (OSError, ValueError):
        info = {}
        missing.append("板端运行元数据不可读")
    sources = [(name, path, relative) for name, base in roots.items()
               for path, relative in selected(base, output, epoch)]
    sources.extend(("runtime", path, path.relative_to(roots["runtime"])) for path in cameras)
    if ("reports" not in roots or not any(name == "reports" for name, _, _ in sources)):
        missing.append("轮次报告不可用")
    if not any(path.suffix == ".jsonl" for _, path, _ in sources):
        missing.append("没有可回放的结构化事件")
    with open(output, "xb", opener=lambda path, flags: os.open(path, flags, 0o600)) as stream:
        with tarfile.open(fileobj=stream, mode="w:gz") as archive:
            def add(name, raw):
                member = tarfile.TarInfo(name)
                member.size, member.mode = len(raw), 0o600
                archive.addfile(member, io.BytesIO(raw))
            for name, path, relative in sources:
                try:
                    raw = path.read_bytes()
                    if path.suffix == ".json":
                        value = redact(json.loads(raw))
                        if isinstance(value, dict) and isinstance(value.get("map_digest"), str):
                            map_digests.add(value["map_digest"])
                        raw = canonical(value).encode()
                    if path.suffix == ".jsonl":
                        rows = []
                        for line in raw.splitlines():
                            try:
                                event = redact(json.loads(line))
                                if isinstance(event.get("time"), (int, float)):
                                    timestamps.append(event["time"])
                                payload = event.get("payload", {})
                                if not isinstance(payload, dict):
                                    payload = {}
                                for candidate in (payload, payload.get("command", {}), payload.get("plan", {})):
                                    if isinstance(candidate, dict) and isinstance(candidate.get("map_digest"), str) and candidate["map_digest"]:
                                        map_digests.add(candidate["map_digest"])
                                rows.append(canonical(event).encode())
                            except (ValueError, TypeError, AttributeError):
                                missing.append(f"{relative} 中有无法读取的记录")
                        raw = b"\n".join(rows) + b"\n"
                except (OSError, ValueError, TypeError) as error:
                    missing.append(f"{relative}：{type(error).__name__}")
                    continue
                arcname = f"{name}/{relative.as_posix()}"
                add(arcname, raw)
                files.append({"path": arcname, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)})
            manifest = {
                "format": 1, "round_epoch": epoch, "robot_id": info.get("robot_id"),
                "software": info.get("software", {}), "created": time.time(),
                "map_digests": sorted(map_digests), "time_range": [min(timestamps), max(timestamps)] if timestamps else [],
                "missing": sorted(set(missing)), "files": files,
            }
            add("manifest.json", canonical(manifest).encode())
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runtime", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--agent", type=Path, help="include a separately configured legacy Agent directory")
    parser.add_argument("--consensus", type=Path, help="include a separately configured legacy Raft directory")
    parser.add_argument("--reports", type=Path)
    parser.add_argument("--round", dest="epoch", type=int)
    args = parser.parse_args()
    if not args.runtime.is_dir():
        parser.error("runtime directory does not exist")
    reports = args.reports or args.runtime.parent / "reports"
    export(args.runtime, args.output, args.epoch, reports,
           {name: path for name, path in {"agent": args.agent, "consensus": args.consensus}.items() if path})
