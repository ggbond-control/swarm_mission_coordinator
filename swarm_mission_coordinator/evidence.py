"""Export explicitly selected local swarm runtime evidence; omit credentials."""

import argparse
import tarfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="Collect local swarm runtime without credential-bearing files"
    )
    parser.add_argument("runtime", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    root = args.runtime.resolve()
    with tarfile.open(args.output, "w:gz") as archive:
        for path in root.rglob("*"):
            if (
                path.is_file()
                and not path.is_symlink()
                and path.resolve() != args.output.resolve()
                and path.name != "credentials.json"
                and not path.name.startswith("private_")
                and "private" not in path.parts
                and path.suffix in {".log", ".json", ".html", ".yaml"}
            ):
                archive.add(path, arcname=str(path.relative_to(root)), recursive=False)
