.. toctree::
   :maxdepth: 2
   :caption: Contents:

Installation
=================


This tool requires an access to IHME clutser and internal pypi server. To install this package, create or edit a file called ~/.pip/pip.conf which looks like this
::

   [global]
   extra-index-url = http://dev-tomflem.ihme.washington.edu/simple
   trusted-host = dev-tomflem.ihme.washington.edu

This file tells the pip package management system to check with IHME's internal pypi server for packages.

You can then install this package with
::

    > source activate <your-env-name>
    > pip install vivarium-cluster-tools

In addition, this tool needs redis client and cython.
::

   > conda install redis cython
