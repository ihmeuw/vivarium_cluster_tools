**2.1.17 - 08/01/25**

  - Use vivarium_dependencies for common setup constraints

**2.1.16 - 07/25/25**

  - Feature: Support new environment creation via 'make build-env'

**2.1.15 - 07/16/25**

  - Support pinning of vivarium_build_utils; pin vivarium_build_utils>=1.1.0,<2.0.0

**2.1.14 - 06/16/25**

  - Type-hinting: Fix mypy errors in psimulate/cluster/cli_options.py

**2.1.13 - 06/11/25**

  - Type-hinting: Fix mypy errors in utilities.py

**2.1.12 - 06/11/25**

  - Move deprecated rtd for redis


**2.1.11 - 06/11/25**

  - Type-hinting: Fix mypy errors in perf_report.py

**2.1.10 - 06/09/25**

  - Type-hinting: Fix mypy errors in logs.py
  - Type-hinting: Fix mypy errors in cli_tools.py

**2.1.9 - 05/30/25**

  - Type-hinting: Fix mypy errors in conftest.py

**2.1.8 - 05/28/25**

  - CI: Enable running mypy

**2.1.7 - 05/27/25**

  - Feature: Remove duplicate log messages to main simulation log file

**2.1.6 - 05/01/25**

  - Bugfix: Add sim verbosity click option to restart and expand commands
  - Refactor: Refactor vivarium_work_horse into smaller units

**2.1.5 - 04/15/25**

  - Make logging verbosity for individiual simulations configurable

**2.1.4 - 02/05/25**

  - Get python versions from python_versions.json
  
**2.1.3 - 02/03/25**

  - Revert change caused by isort version

**2.1.2 - 01/31/25**

  - Linting  

**2.1.1 - 12/16/24**

  - Defer defining randomness additional seed to RandomnessManager

**2.1.0 - 11/06/24**

  - Drop support for Python 3.9
  - Modernize type hinting

**2.0.4 - 10/07/24**

  - Update how vivarium work horse gets number of simulation steps

**2.0.3 - 09/10/24**

 - Bugfix for job_parameters with simulation backups

**2.0.2 - 08/30/24**

 - Strengthen the API documentation
 - Update existing documentation to include new psimulate options

**2.0.1 - 08/21/24**

 - Use script to install dependencies in CI

**2.0.0 - 08/13/24**

Breaking changes:
 - Implement Vivarium's new results processing

Major changes:
 - Add ability to back up simulations periodically during runs, and to restart from these backups

Other changes:
 - Use external layered-config-tree package instead of Vivarium ConfigTree class
 - Raise instead of log when there are more jobs than expected from a previous run
 - Stop installing vivarium inside the package directory during github actions
 - Automatically run Jenkins builds on push or pull request

**1.6.1 - 06/17/24**

 - Hotfix pin numpy below 2.0

**1.6.0 - 03/12/24**

 - Limit max number of draws to 500.

**1.5.6 - 02/27/24**

 - Give user option to continue psimulate restart if environment has changed

**1.5.5 - 02/12/24**

 - Add central performance logging and testing

**1.5.4 - 01/29/24**

 - Fix broken readthedocs build

**1.5.3 - 01/04/24**

 - Hard-code psimulate test cluster requirements
 - Implement hardware constraint option for psimulate commands
 - Make -P/--project a psimulate required option

**1.5.2 - 12/29/23**

 - Automatically remove duplicative perf logs after log_summary.csv is created

**1.5.1 - 12/15/23**

 - Add logging documentation for psimulate

**1.5.0 - 10/27/23**

 - Remove default results directory for 'psimulate run'
 - Modify default output directory for 'psimulate test'
 - Add --max-workers default of 8000

**1.4.3 - 10/25/23**

 - Bugfix implement --max-workers option for psimulate restart, expand, and test
 - Bugfix update default output directory
 - Update runtime error message if env is different on restart or expand

**1.4.2 - 10/13/23**

 - Bugfix checking for max_workers arg

**1.4.1 - 10/13/23**

 - Bugfix checking for max_workers arg

**1.4.0 - 10/09/23**

 - Drop support for Python 3.8
 - Add priorities for draw/seed/branch job groups

**1.3.13 - 10/02/23**

 - Bugfixes for psimulate introduced by previous bugfix

**1.3.12 - 09/22/23**

 - Bugfixes for psimulate

**1.3.11 - 09/07/23**

 - Made job failures more prominent in end of jobs logging

**1.3.10 - 07/12/23**

 - Allow for specifying random seeds and draws in branches file
 - Bugfixes for make_artifacts -l all
 - Changes version metadata to use setuptools_scm
 - Increments version of Vivarium required

**1.3.9 - 06/01/23**

 - Increments version of Vivarium required
 - Prevent sorting of model specification keys


**1.3.8 - 12/27/22**

 - Updates CI and setup to build python 3.7-3.10
 - Updates codeowners

**1.3.7 - 10/28/22**

 - Adds a test `psimulate test large_results` for scale testing
 - Sets the default project to `proj_simscience_prod`

**1.3.6 - 10/04/22**

 - Increments version of Vivarium required 

**1.3.5 - 09/20/22**

 - Standardize results directories
 - Adds ability to run without artifact
 - Specify correct permissions when creating directories and files

**1.3.4 - 07/01/22**

 - Mend key mismatch for random_seed and input_draw columns
 - Make draw and seed generation deterministic in parallel simulations
 - Add CODEOWNERS to repo

**1.3.3 - 05/16/22**

 - Add results directory to model specification in psimulate runs.

**1.3.2 - 05/05/22**

 - Fix pandas json deprecation.
 - Fix new slurm atexit error.
 - Update black version used in CI.

**1.3.1 - 03/30/22**

 - Fix output directory naming based on location.

**1.3.0 - 02/28/22**

 - Refactor and reorganize codebase.
 - Add load testing capabilities.
 - Switch from UGE to SLURM.
 - Fix failure accounting in the registry.
 - Fail earlier from bad arguments.

**1.2.13 - 02/15/22**

 - Autoformat code with black and isort.
 - Add black and isort checks to CI.

**1.2.12 - 02/11/22**

 - Update pip freeze behavior to be more robust
 - Add logging and error handling when the node with the main process can't view the filesystem.

**1.2.11 - 02/11/22**

 - CI configuration updates.

**1.2.10 - 10/28/21**

 - Add Zenodo metadata and update license to BSD 3-clause

**1.2.9 - 10/14/21**

 - Set log-level to debug for both redis server and workers
 - Create separate redis logs for each redis server
 - Fix bug inherited from rq hiding worker processes from scheduler
 - Fix incorrect help text for psimulate max-runtime flag

**1.2.8 - 08/16/21**

 - Don't write sim results in worker logs.

**1.2.6 - 08/10/21**

 - Standardize CI scripts
 - Upgrade readthedocs
 - Add API documentation
 - Allow psimulate to create result root directories

**1.2.5 - 06/08/21**

 - Unpin redis and rq dependencies

**1.2.4 - 05/12/21**

 - Add artifact path as an argument to psimulate
 - Fix redis connection bug
 - Add no_cleanup option to prevent auto-deletion on a failure
 - Add additional timing messages in the worker logs
 - Add JSON telemetry logging for each job run
 - Add vipin tool which logs job stats and outputs them to a csv or hdf file
 - Remove vparse functionality which is superseded by vipin
 - Remove references to deprecated DataFrame's msgpack functionality

**1.2.3 - 01/05/21**

 - Fix deploy script

**1.2.2 - 01/05/21**

 - Github actions replaces Travis for CI
 - Unpin pandas and numpy

**1.2.1 - 10/02/20**

 - Pin rq to 1.2.2

**1.2.0 - 08/31/20**

 - Update output.hdf to remove duplicate draw and random seed from the index.
 - Adds option for choosing scheduling queue.
 - Adds typing to internal functions.
 - Removed references to old IHME cluster.
 - Separated run configuration from cluster configuration.
 - Added utility function to make directories with consistent permissions.
 - Added functionality to serialize unwritten in-memory results on exit.
 - Added functionality to clean up directories if psimulate fails to produce results.

**1.1.2 - 01/03/20**

 - Set cluster queue dynamically based on max runtime argument.

**1.1.1 - 12/08/19**

 - Fix bug in random seed generation so that seeds are unique.

**1.1.0 - 11/18/19**

 - Move artifact from vivarium_public_health to vivarium proper. Remove the
   package dependency.
 - Clean up context interface and simulation creation.
 - Switch all logging to loguru.

**1.0.15 - 09/11/2019**

 - Add max runtime option
 - Set output directory permissions to 775
 - Prohibit launching from submit host

**1.0.14 - 06/20/19**

 - Bugfix in job enqueuing.

**1.0.12 - 06/20/19**

 - Bugfix in worker invocation

**1.0.11 - 06/19/19**

 - Namespace bugfix.

**1.0.10 - 06/18/19**

 - Added option to not batch results.
 - Updated yaml api usage.
 - Bugfix in dtypes when writing results.
 - Allow the usage of branch files without parameter variations.
 - Don't use ``ResultsWriter`` directly.
 - Switch to consistent pathlib usage.
 - Enable restart when no parameter variations present.
 - Setup log rotations.
 - Forward vivarium logs to worker logs.
 - New ``vparse`` command for parsing worker logs.

**1.0.9 - 04/22/19**

 - Bugfix in restart with string columns in outputs.
 - Extract common cli options.
 - Add verbosity levels to logging.
 - Add command to add draws/seeds to previous runs.

**1.0.8 - 04/16/19**

 - Switch to loguru for logging and cleanup usage.
 - Log cluster and node information from the workers.
 - Add serialized logs.
 - Client side sharding of redis instances.
 - More robustness in failure handling.
 - Tutorial documentation for yaml syntax.
 - Tutorial documentation for branches files.
 - Tutorial documentation for psimulate.
 - Readthedocs integration.
 - Extraction of shared CLI options

**1.0.7 - 04/02/19**

 - Be defensive about retrieving jobs.

**1.0.6 - 03/29/19**

 - Bugfix in tests.

**1.0.5 - 03/29/19**

 - Migrate to github.

**1.0.4 - 03/28/19**

 - Add debugger to cli.

**1.0.3 - 03/20/19**

 - Reduce requirements for simulation jobs.

**1.0.2 - 03/19/19**

 - Hack around hard to reproduce drmaa error.

**1.0.1 - 03/15/19**

 - Add additional project options.

**1.0.0 - 02/22/19**

 - Initial release.
