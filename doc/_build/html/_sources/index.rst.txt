About Orange3 Databricks Add-ons
================================

Orange Databricks add-on is an ordinary python package, installed by running setup.py install and
distributed via pypi. Widgets, tutorials and help are registered in setup.py file using
entry points. When Orange Canvas starts, it reads this entry points, registers all the
widgets and adds them to the menu. This contains one widget (Databricks), which is
contained in a new category (Databricks). The widget also displays help when you press F1.

Contents:

.. toctree::
   :maxdepth: 2

Widgets
-------

.. toctree::
   :maxdepth: 1

   widgets/databrickswidget

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

