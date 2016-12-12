# mids-251-web-crawler
A repository for all code written for the MIDS-251 Final project by Chuck, Rama, Chris, and Andrew.

This web crawler is a closed domain crawler for information on medical doctors and procedures. It provides simple query capabilities for users intersted in learning more about a specific doctor or procedure. The application can be categorized into four steps:

 - Provided a set of seed URLs, the web crawler will identify all URLs referenced in each seed and stores them. The crawler is also configurable to recursively process each new URL and will do so for as many levels of recursion that is specified.
 - Download HTML from each valid URL provided by previous step and store content in a MongoDB NoSQL Document Store.
 - Parse HTML documents stored in MongoDB to extract meaningful keywords and store results into Cassandra, a NoSQL database.
 - Using a simple query interface, extract and serve the URLs that are relevant to the keyword search.


### Architecture


### Repository

Additional information about the contents of each folder can be found in the README file in the folders.


### Instructions