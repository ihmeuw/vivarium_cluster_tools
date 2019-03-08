.. toctree::
   :maxdepth: 2
   :caption: Contents:

How to use distributed runner?
===============================

Once you successfuly make your simulation specification and branch file, now it is actual time to use the distributed runner.
You may remember how we run a single simulation with the model specification file. It was,

.. code-block:: console

    simulate run /path/to/your/model/specification

Very similar to this, we have a command ``psimulate`` with meaning of simulate parallelly. You can run your distributted runner by

.. code-block:: console

    psimulate run /path/to/your/model/specification  /path/to/your/branch

By default, output will be saved in ``/share/scratch/users/{your_user_id}/vivarium_results``. If you want to save the results
somewhere else, you can specify your output directory where the final result to be saved as an optional argument.

.. code-block:: console

    psimulate run /path/to/your/model/specification /path/to/your/branch -o /path/to/output

Another optional argument that you can choose is project. By default, it uses ``proj_cost_effect``. If you are working on the
csu project, you may specify as

.. code-block:: console

    psimulate run /path/to/your/model/specification /path/to/your/branch -p proj_csu

Currently, ``proj_cost_effect`` and ``proj_csu`` are the only options that you can select.


If your ``psimulate run`` has any failed jobs from the previous runs, you can restart failed jobs by specifying
which output directory includes the partially completed jobs by,

.. code-block:: console

    psimulate restart /path/to/the/previous/results/

For the ``psimulate restart``, you can also choose a project with optional flag `-p` as same as ``psimulate run``.
