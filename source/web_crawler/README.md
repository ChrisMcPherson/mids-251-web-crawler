# Web Crawler

### Step 1 

Step 1 of the web crawler identifies all URLs referenced in each provided seed and stores them for the next step of the crawler. This is a recursive process that will continue for as many times that is configured.


### Step 2

Step 2 of the web crawler downloads HTML from each valid URL provided by previous step and stores content in a MongoDB NoSQL Document Store.
The URLs from Step 1 are dropped off into the /root/url_lists/ directory in files named "logfile1.txt" (with the integer representing the file count). Use the below bash commands to process these URLs. The shell script executed in the below command will take all URL seed files in /root/url_lists/, combines them, splits the combined list into 300 equal length files, and starts 300 parallel instances of the Python script that downloads the HTML and uploads the content to MongoDB.

```sh
$ cd /root/urls/
$ bash split_file_and_parallelize.sh
```