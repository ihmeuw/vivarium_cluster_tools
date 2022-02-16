Vivarium Cluster Tools
=======================

.. image:: https://badge.fury.io/py/vivarium-cluster-tools.svg
    :target: https://badge.fury.io/py/vivarium-cluster-tools

.. image:: https://travis-ci.org/ihmeuw/vivarium_cluster_tools.svg?branch=main
    :target: https://travis-ci.org/ihmeuw/vivarium_cluster_tools
    :alt: Latest Version

.. image:: https://readthedocs.org/projects/vivarium-cluster-tools/badge/?version=latest
    :target: https://vivarium-cluster-tools.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

Vivarium cluster tools is a python package that makes running ``vivarium``
simulations at scale on a Univa Grid Engine cluster easy.

Installation
------------

You can install this package with

.. code-block:: console

    pip install vivarium-cluster-tools

In addition, this tool needs the redis client. This must be installed using conda.

.. code-block:: console

    conda install redis

A simple example
----------------

If you have a ``vivarium`` model specifcation file defining a particular model,
you can use that along side a **branches file** to launch a run of many
simulations at once with variations in the input data, random seed, or with
different parameter settings.

.. code-block:: console

    psimulate run /path/to/model_specification.yaml /path/to/branches_file.yaml

The simplest branches file defines a count of input data draws and random seeds
to launch.

.. code-block:: yaml

    input_draw_count: 25
    random_seed_count: 10


This branches file defines a set of simulations for all combinations of 25
input draws and 10 random seeds and so would run, in total, 250 simulations.

You can also define a set of parameter variations to run your model over. Say
your original model specification looked something like

.. code-block:: yaml

    plugins:
      optional: ...

    components:
      vivarium_public_health:
        population:
          - BasePopulation()
          - Mortality()
        disease.models:
          - SIS('lower_respiratory_infections')
      my_lri_intervention:
        components:
          - GiveKidsVaccines()

    configuration:
      population:
        population_size: 1000
        age_start: 0
        age_end: 5
      lri_vaccine:
        coverage: 0.2
        efficacy: 0.8

Defining a simple model of lower respiratory infections and a vaccine
intervention. You could then write a branches file that varied over both
input data draws and random seeds, but also over different levels of coverage
and efficacy for the vaccine.  That file would look like

.. code-block:: yaml

    input_draw_count: 25
    random_seed_count: 10

    branches:
      lri_vaccine:
        coverage: [0.0, 0.2, 0.4, 0.8, 1.0]
        efficacy: [0.4, 0.6, 0.8]

The branches file would overwrite your original ``lri_vaccine`` configuration
with each combination of coverage and efficacy in the branches file and launch
a simulation. More, it would run each coverage-efficacy pair in the branches
for each combination of input draw and random seed to produce 25 * 10 * 5 * 3 =
3750 unique simulations.

To read about more of the available features and get a better understanding
of how to correctly write your own branches files, check out the
`vivarium cluster tools documentation <https://vivarium-cluster-tools.readthedocs.io/en/latest/?badge=latest>`_.
