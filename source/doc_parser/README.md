# Search Instructions:

Running the Search.

Version 1:
Change to the directory where UberSearch-0.0.1-SNAPSHOT-jar-with-dependencies.jar is located and run the following command
```sh
$ java -cp UberSearch-0.0.1-SNAPSHOT-jar-with-dependencies.jar UberSearch <search term> <thread count> <number of documents>
```
Version 2:
```sh
$ ./UberSearch.sh <searchId> <searchTerm> 
```

Compling the code or building the jar file

1. Create a Maven Project, copy the java files and pom.xml

2. Clean compile assemble:single

3. Copy the jar file to the server where mongo server is running