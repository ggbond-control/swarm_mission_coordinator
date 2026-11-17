from glob import glob

from setuptools import setup

name = "swarm_mission_coordinator"
setup(
    name=name,
    version="0.1.0",
    packages=[name],
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + name]),
        (
            "share/" + name,
            ["package.xml", "README.md", "TEST_LOG.md"] + glob("PROTOCOL.md"),
        ),
        (
            "share/" + name + "/examples",
            glob("examples/*.html") + glob("examples/*.json"),
        ),
    ],
    install_requires=["setuptools"],
    zip_safe=False,
    entry_points={
        "console_scripts": [
            "swarm_mission_coordinator_node = swarm_mission_coordinator.node:main",
            "swarm_collect_evidence = swarm_mission_coordinator.evidence:main",
            "swarm_prepare_demo = swarm_mission_coordinator.demo:main",
        ]
    },
)
