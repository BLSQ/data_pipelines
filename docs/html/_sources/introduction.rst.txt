General Description
===================================================

Our work is organized around three main dimensions:
* Pipelines of data processing in which we develop and organized our data sciences methods.
* Building blocks in which we adapt our processes to our specific knowledge of health systems and health issues in developing countries.
* Projects in which we combine pipelines from different building blocks, and add custom work for a specific objective and outcome.

For example, our work on data auditing and data modeling allows gives us a good understanding of how to analyze and represent HMIS data availability for patients care and logistical systems, which we will implement in many of our project, with some adaptation regarding the subjects we are more interested in (HIV, family planning...)

Data Pipelines
------------------------

Pipelines are the generic manipulation we are doing in most of the project we are working. They are our core toolbox, and are developed overtime when we have to process data for different purposes. We are currently organized around five  main pipelines.

Data Audit
~~~~~~~~~~~~~~~~~~~~~~~~~
In this pipeline we work on understanding what a data source includes, and what can or can't be done with it. Most of the HMIS data we work with includes hundreds of indicators, measured over time in thousands of facilities. We develop a generic approach and tools to assess data quality, and explore possible usages and potential shortcomings, even before we start comprehensively processing the data.

Data Mapping
~~~~~~~~~~~~~~~~~~~~~~~~~
Because we do a lot of data compiling from different sources, we need to map those sources between them (indicators, orgUnits…), mapping indicators with concepts etc… This includes some help for manual integration, and some machine learning approach for automating burdensome tasks.

Data Transform
~~~~~~~~~~~~~~~~~~~~~~~~~
Once data has been identified and mapped, we need to transform it before it is modeled. Due to type of real life data we use, this is a really not trivial line of work. This includes data sources reconciliation, data imputation, data selection...

Data Modeling
~~~~~~~~~~~~~~~~~~~~~~~~~
The data modeling is all the analytical work we want to do with the data to produce some value. This includes building composite indicators, any kind of statistical modeling or learning we are doing on a routine basis and want to replicate across different projects because it is good.

Data Visualization
~~~~~~~~~~~~~~~~~~~~~~~~~
Finally, we have to format outputs to fit both the needs of our clients, and the tools we use to communicate or present our work. This includes graphs, shiny apps, notebooks...

Building Blocks
----------------------------
Building blocks are the different domains of analysis we are interested in investigating. For each of the pipelines, some of the processes have different approaches depending on the building blocks they refer to. For example, when working on HIV data, the way we impute missing data for a number of active patients on ART will be different from how we impute data on ART stocks.

Patients visits
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Health inputs
~~~~~~~~~~~~~~~~~~~~~~~~~~

Infrastructure
~~~~~~~~~~~~~~~~~~~~~~~~~~

Human Resources
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. todo:: Add diagram to illustrate pipeline x blocks Matrix

Projects
----------------------------

.. todo:: Add links to projects outputs or notebooks.
