"""
======
dagger
======

CLI for running multi-step Jobmon workflows defined by a YAML config.
Separate from :mod:`vivarium_cluster_tools.psimulate` so that workflow
orchestration is not bundled with the simulation-runner entry points,
while still sharing the underlying workflow-config parsing, Jobmon
client glue, and notification machinery.
"""
