.. toctree::
   :maxdepth: 2
   :caption: Contents:

How to use distributed runner?
===============================

Once you successfuly make your simulation configuration and branch file, now it is actual time to use the distributed runner.
You may remember how we run a single simulation with the configuration file. It was,

.. code-block::
simulate run <path-to-your-configuration-file>




The ``vivarium`` ecosystem uses YAML configuration files throughout, including ``vivarium-cluster-tools``.
To run the multiple scenarios, you need to make a separate branch file (YAML) in addition to a usual
vivarium model specification YAML file. You can specify how to vary a certain parameter in this branch file.

For example,

::

    input_draw_count: 25
    random_seed_count: 10

    branches:
      - intervention_name:
           intervention_coverage: [0, 1]
           efficacy: [0, .5, 1]

This branch shows that you want to have 25 different input draws, 10 different random seeds and 2 different
intervention coverages and 3 different intervention efficacy. This implies 25*10*2*3=1500 different simulation
scenarios. You can run all 1500 simulations by

::

    psimulate run /path/to/your/model_configuration /path/to/your/branch_file

As an optional parameter, you can specify the name of project as well as result directory to save the outputs
and logs. To specify those optional arguments,

::

    psimulate run /path/to/your/model_configuration /path/to/your/branch_file -p project_name -o /path/to/output/

If your psimulate run has any failed jobs from the previous runs, you can restart failed jobs by specifying
which output directory includes the partially completed jobs by,

::

    psimulate restart /path/to/the/previous/results/

