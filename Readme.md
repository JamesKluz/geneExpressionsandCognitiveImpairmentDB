#Big Data Project #1
CSCI 493.71, Spring 2017

Professor Lei Xie

### Authors
- James Kluz
- Gil Dekel

### Python 3

### Runtime Instructions
1. From the Kluz_Dekel folder run python3 main.py
2. If the database tables/graph have not been loaded do the following:
    a) Enter '1' to load all tables and graphs from scratch (it takes approximately 14 minutes to load the entire database)
3. From here you can just follow the prompts on the screen to add/remove items from the databases and make queries

The program itself expects the following:
1. The following files are in folder Kluz_Dekel/datafiles: BIOGRID-MV-Physical-3.4.144.tab2 entrez_ids_uniprot.txt patients.csv ROSMAP_RNASeq_entrez.csv uniprot-human.xml
2. The following libraries/packages are installed: neo4jrestclient, MySQLdb, mysqlclient, mySQL, Python3, java 8, mongoDb, lxml, xmljson, pandas
3. A neo4j database is running and set with the following parameters: (this can be changed by changing the variables at the top of the neo4j.py file)
      The database path is set to: "http://localhost:7474"
      The username is set to: "neo4j"
      The password is set to: "jamesandgil"
4. An empty (or not) mySQL database has been created named BIG_DATA_PROJECT_1 with password "52637dualred" (these parameters can be changed in entrez_uniprot_sql.py)
5. mongod has been run 

### Databases
This project uses MongoDb, Neo4j and SQL. The program requires that an instance of both MongoDb and Neo4j databases are running locally before execution (see details above). 
