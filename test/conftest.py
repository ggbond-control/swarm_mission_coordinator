"""Source-only tests are independent of the old four cooperation packages."""

import sys
from pathlib import Path

for package in Path(__file__).resolve().parents[2].glob("swarm_*"):
    if package.is_dir():
        sys.path.insert(0, str(package))
