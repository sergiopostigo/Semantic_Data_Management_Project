# Semantic_Data_Management_Project

## Requirements
- Java 11
- GraphDB

## Create turtle file
There are multiple functoins in the ``Main.java`` file. Most of them are commented and usefull only for development porpouses.
The function that is by default not commented is the one used to export the graph into a Turtle file.

To run this project, clone it, compile the Maven Project and run the ``Main.java`` file.
This will create a turtle file containing the triplets to build the graph.

## Populate the data base
To upload the graph into graphDB:
- at the opening window, open settings and allow imports for more than 200 MB to setting ``graphdb.workbench.maxUploadSize = 40000000000``
- in graphDB create a repository and import the turtle file. 

## Query the graph
An example of a query on the data is provided by ``query_radiators_from_USA.txt``. This will return all the items classifed as RADIADORES from the country UNITED STATES. Similar queries can be easily build on the top of this one.
