README
===

Hadoop project, MapReduce, Data Analysis, Java

This project is to use Java to manuplate multiple text files with selecting, projection, natural joint and aggregation. All these operations are done with MapReduce process instead of database.

The target of the project is to let us get familar with the MapReduce machnism, how to design reasonable Map and Reduce function, and how to dispatch/deploy the tasks in Hadoop.

For details, check the *ProjectDescription.pdf*

##GUIDE

This folder stores the necessary data source, code as well as the output for the *Advanced Database* Project, the structure is as follows:

*data*: the source data file that stores the manuplated data(city.txt, country.txt, countrylanguage.txt)

*output*: the output result of each experiments, there is a flag file indicating the status, and the other `part-r-00000` is the generated result.

*src*: the source code of the project written in `Java`, for each seperate task there is a package named `ex1`, `ex2`,`ex3`, `ex4`.

*INSTALL_GUIDE_2nd.pd*: the guide that I referred when configuring the **Hadoop** environment

##Contact

**Email**: Chang Liu(chang_liu@student.uml.edu)

**Student ID**: 01516178
