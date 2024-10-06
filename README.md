Orange3 Databricks Add-on
=========================

This is a Databricks add-on for [Orange3](http://orange.biolab.si). It adds a widget to pull data from a Databricks instance.

Installation
------------

To install the add-on from source run

    pip install .

To register this add-on with Orange, but keep the code in the development directory (do not copy it to Python's site-packages directory), run

    pip install -e .

Documentation / widget help includes some markdown and thus requires [MyST-Parser](https://myst-parser.readthedocs.io/en/latest/). Install with `pip install --upgrade myst-parser` and add *myst_parser* to the [list of configured extensions](https://www.sphinx-doc.org/en/master/usage/configuration.html#confval-extensions) as per [here](https://www.sphinx-doc.org/en/master/usage/markdown.html).

Then documentation can be built by running

    make html htmlhelp

from the doc directory.

Usage
-----

After the installation, the widget from this add-on is registered with Orange. To run Orange from the terminal,
use

    orange-canvas

or

    python -m Orange.canvas

The new widget appears in the toolbox bar under the section Databricks.

![screenshot](https://github.com/the-mad-statter/orange3-databricks-addon/blob/main/screenshot.png)
