"""Offline migration from per-package Swarm runtime roots."""

import argparse
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import time

from swarm_protocol.storage import ProcessLock, atomic_json

COMPONENTS = ("coordinator", "agent", "consensus", "transport")


def checksum(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest() if hasattr(hashlib, "file_digest") else hashlib.sha256(source.read()).hexdigest()


def moved_path(value, source, mapping):
    original = Path(value)
    for candidate in (original.resolve(), (source.parent / original).resolve()):
        if candidate in mapping:
            return mapping[candidate]
    # If cooperation_workspace moved an old repository, recorded absolute paths
    # may still refer to its old location. Use a matching path suffix only when
    # it identifies exactly one archived file.
    matches = [target for path, target in mapping.items()
               if path.name == original.name and path.parent.name == original.parent.name]
    return matches[0] if len(matches) == 1 else None


def migration_files(sources):
    for component, source in sources.items():
        if source is None:
            continue
        if not source.is_dir():
            raise ValueError(f"源目录不存在：{source}")
        for path in sorted(source.rglob("*")):
            rel = path.relative_to(source)
            if path.is_symlink():
                if rel.parts[0] in {"validation", "ros_logs"}:
                    continue  # historic workspace links and ROS latest are not recovery files
                raise ValueError(f"请先核对符号链接：{path}")
            if not path.is_file() or path.name == "process.lock":
                continue
            target = Path("runtime", component, rel)
            if component == "coordinator":
                if rel.parts[0] in {"validation", "ros_logs", "runtime-info.json"}:
                    target = Path("runtime", rel)
                elif rel.parts[0] == "reports":
                    if rel.name == "job.json" or rel.name == "credentials.json" or rel.name.startswith("upload-"):
                        target = Path("runtime/coordinator/private_uploads", *rel.parts[1:])
                    else:
                        target = rel
            yield path, target


def migrate(sources, destination, *, apply=False):
    destination = destination.resolve()
    report_root = destination.parent / "reports"
    sources = {k: p.resolve() if p else None for k, p in sources.items()}
    same_root = sources.get("coordinator") == destination
    if destination.exists() and not same_root and any(destination.iterdir()):
        raise ValueError("目标 runtime 已有内容")
    if report_root.exists() and any(report_root.iterdir()):
        raise ValueError("目标 reports 已有内容")
    if same_root and any((destination / key).exists() for key in COMPONENTS):
        raise ValueError("目标已经是统一目录，不能再次从该路径迁移")
    if any(p and (p == report_root or p.is_relative_to(destination) and p != destination)
           for p in sources.values()):
        raise ValueError("源目录与目标重叠")
    operations = list(migration_files(sources))
    if not operations or len({target for _, target in operations}) != len(operations):
        raise ValueError("没有可迁移数据或目标文件重复")
    print(json.dumps({"destination": str(destination), "files": len(operations),
                      "sources": {k: str(p) for k, p in sources.items() if p}}, ensure_ascii=False))
    if not apply:
        return None
    with ExitStack() as stack:
        for source in dict.fromkeys(p for p in sources.values() if p):
            lock = ProcessLock(source / "process.lock")
            stack.callback(lock.close)
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Keep a checked copy of the old coordinator root if the destination
        # occupies the same path. Other old roots remain untouched.
        with tempfile.TemporaryDirectory(dir=destination.parent, prefix=".swarm-migrate-") as temp:
            stage = Path(temp)
            mapping = dict(operations)
            manifest = []
            for src, relative in operations:
                dst = stage / relative
                dst.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                old_hash = checksum(src)
                shutil.copy2(src, dst)
                if "private_uploads" in relative.parts or dst.name.startswith("private_"):
                    dst.chmod(0o600)
                if checksum(dst) != old_hash:
                    raise ValueError(f"文件校验失败：{src}")
                if dst.name == "job.json" or dst.name == "execution.json" or dst.suffix == ".json" and "route_archive" in dst.parts:
                    record = json.loads(dst.read_text())
                    field = "path" if dst.name == "job.json" else "yaml_path"
                    old = record.get(field)
                    target = moved_path(old, src, mapping) if old else None
                    if target:
                        record[field] = str(destination.parent / target)
                        atomic_json(dst, record)
                    elif old and (dst.name == "execution.json" or
                                  dst.name == "job.json" and record.get("status") in {"pending", "uploading"}):
                        raise ValueError(f"恢复记录引用了缺失文件：{src}")
                manifest.append({"old": str(src), "new": str(relative), "sha256": old_hash,
                                 "new_sha256": checksum(dst)})
            backup = None
            if same_root:
                backup = destination.with_name(f"runtime-before-{time.time_ns()}")
            atomic_json(stage / "runtime" / "migration.json",
                        {"backup": str(backup) if backup else None, "files": manifest})
            if backup:
                destination.rename(backup)
            elif destination.is_dir():
                destination.rmdir()
            installed_report = False
            try:
                if (stage / "reports").exists():
                    if report_root.is_dir():
                        report_root.rmdir()
                    (stage / "reports").rename(report_root)
                    installed_report = True
                (stage / "runtime").rename(destination)
            except OSError:
                if installed_report:
                    report_root.rename(stage / "reports")
                if backup:
                    backup.rename(destination)
                raise
            return destination / "migration.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", required=True, type=Path)
    for component in COMPONENTS:
        parser.add_argument("--" + component, type=Path, help="old " + component + " root")
    parser.add_argument("--apply", action="store_true", help="copy after all cooperation processes have stopped")
    args = parser.parse_args()
    try:
        saved = migrate({key: getattr(args, key) for key in COMPONENTS}, args.runtime, apply=args.apply)
        if saved:
            print(saved)
    except (ValueError, OSError, RuntimeError) as error:
        parser.error(str(error))
