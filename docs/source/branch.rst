
The branches file
=================

When you design a model specification, you are often interested in simulating many different scenarios of the model that
correspond to changes in the parameters you have defined. Let's assume that you have such a such configuration block in
your model specification file

.. code-block:: yaml

    configuration
            shigellosis_vaccine:
                        doses: ['first', 'second', 'booster', 'catchup']
                        dose_response:
                                onset_delay: 14  # Days
                                duration: 1080  # Days
                                waning_rate: 0.013  # Percent/Day
                        protection:
                                efficacy:
                                        mean : 0.7
                                        standard_error: 0.15
                                dose_protection:
                                        first: 0.7
                                        second: 1.0
                                        booster: 1.0
                                        catchup: 0.7
                        dose_age_range :
                                first :
                                        start: 270
                                        end: 360
                                second :
                                        start: 450
                                        end: 513
                                booster :
                                        start: 1080
                                        end: 1440
                                catchup :
                                        start: 1080
                                        end: 1440


It specifies that you have 4 doses of ``shigellosis_vaccine`` as first, second, booster and catchup. This vaccine takes
14 days to be effective and lasts 1080 days with decreasing protection by 0.013 percent per day. This vaccine has a
mean efficacy of 0.7 with a standard error of 0.15 Each dose protection and each dose age range are also specified.

If we want to run multiple different scenarios based on this model specification, how can we accomplish this?

For example,

.. code-block:: yaml

    input_draw_count: 10
    random_seed_count: 500

    branches:
            - shigellosis_vaccine:  # Baseline
                    dose_response:
                            duration: 1080  # Days
                            waning_rate: 0.013  # Percent/Day

                    protection:
                            efficacy:
                                    mean : 0.7
                                    standard_error: 0.15
                            dose_protection:
                                    first: 0.0
                                    second: 0.0
                                    booster: 0.0
                                    catchup: 0.0
              population_size: 1000


            - shigellosis_vaccine:  # Optimistic no booster
                    dose_response:
                            duration: 1080  # Days
                            waning_rate: 0.013  # Percent/Day

                    protection:
                            efficacy:
                                    mean : 0.7
                                    standard_error: 0.15
                            dose_protection:
                                    first: 0.7
                                    second: 1.0
                                    booster: 0.0
                                    catchup: 0.0
              population_size: 5000

We could specify ``input_draw_count`` and ``random_seed_count`` atop the file that specify the number of draws and
random seeds to be run. However, if we want to vary any parameter inside of the ``configuration`` block of the model
specification, we will need to make a branches file to specify the different parameters. Since a hyphen means a list
in yaml syntax, each scenario should be separated by ``-``. As you can see from the example below, you need to include
all information that you want to vary from the initial model specification per scenario in the same block start with a
hyphen.

.. code-block:: yaml

    branches:
            - egg_intervention:
                    recruitment:
                        proportion: [0.0, 0.4, 0.8, 1.0]

Here there is a list of [0, 0.4, 0.8, 1] inside of the `` egg_intervention`` block. This actually
makes 4 different scenarios with different recruitment proportion of ``egg_intervention``. This is
very convenient way to make multiple scenarios with different values.

However, it is also important to remember that any configuration option that is natively specified
as a list (e.g., ``shigellosis_vaccine.doses`` in the example configuration above) can **NOT** be
used in the branch file.

Since you now know how to write a branch file, let's examine the following branch file and count the number of
simulations that will be run.

.. code-block:: yaml

    input_draw_count: 10
    random_seed_count: 5

    branches:
            - egg_intervention:
                    recruitment:
                        proportion: [0.0, 0.4, 0.8, 1.0]
                    recruitment:
                        age_start: [0, 0.5, 1, 1.5, 2]

This will generate 10 * 5 * 4 (from recruitment proportion) * 5 (from recruitment age_start), meaning, 1000 simulations
will be run.

One thing that you should remember is, varying the time step, start or end time, or the population size
will make profiling jobs very difficult and runs the risk of breaking our output writing tools. Keep this in
mind when you write a branch file.
