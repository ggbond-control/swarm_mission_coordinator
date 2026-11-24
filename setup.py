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
            ["package.xml", "README.md", "TEST_LOG.md"],
        ),
    ],
    install_requires=["setuptools"],
    zip_safe=False,
    entry_points={
        "console_scripts": [
            "swarm_mission_coordinator_node = swarm_mission_coordinator.node:main",
            "swarm_collect_evidence = swarm_mission_coordinator.evidence:main",
            "swarm_migrate_runtime = swarm_mission_coordinator.runtime_migration:main",
            "swarm_prepare_demo = swarm_mission_coordinator.demo:main",
        ]
    },
)
