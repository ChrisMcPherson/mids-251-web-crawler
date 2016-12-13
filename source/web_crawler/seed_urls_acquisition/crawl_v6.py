# Required imports, will need to be thinned out
import argparse
import requests
import httplib
from urlparse import urlparse
from bs4 import BeautifulSoup
from IPython.display import clear_output
import csv
import re
import sys
import os
import time
import uuid
import base64
import urllib2
import urllib
import requests
import shutil
import lxml.html

# seed_lists_folder = "C:/Users/Chuck B/Desktop/notebooks/crawl/seed_lists"

# Stores fully qualified URLs to process
link_list = []
crawled = []
link_count = 0
err_count = 0
log_filename = "logfile.txt"
log = open(log_filename,"a")
count = 0

print "Starting"

# Saves HTML to a file
# Multiplier allows file to be saved multiple times
def save_html(html, url, multiplier = 1):
    print len(html)
    for i in range(0, multiplier):
    
        # Create unique hash for URL filename
        url_id = base64.urlsafe_b64encode(str(i) + url)
        html_filename = ''.join([html_files_folder, "/", url_id, ".txt"])
        html_code = html.encode('utf-8').strip()
        
        if len(html_code) > 0:
            fout = open(html_filename,'w')
            fout.write(html_code)
            fout.close()      

# Writes final URL to log file
def log_url(url):
    log.write(url + "\n")

# Constructs complete URL from base and link, cleans up
def build_url(base_url, link):
    bad_extensions4 = ['.pdf', '.xml'] #'.asp'
    bad_extensions5 = ['.cgir','.aspx']
    
    # Return blank URL if extension is listed above
    if link[-4:] in bad_extensions4:
      return ""

    if link[-5:] in bad_extensions5:
      return ""
     
    if link[0:5] == 'http:':
      return link
    
    elif link[0:6] == 'https:':
      return link

    # Concatenate base_url and link
    if base_url[-1:] == "/" and link[:1] == "/":
      combo_url = base_url + link[1:]
      
      if combo_url.find("#"):
        return base_url + link[1:]
      else:
        return ""
        
    else:
      combo_url = base_url + link
      
      if combo_url.find("#"):
        return base_url + link
      else:
        return ""

    return ""

# Loop through all seed files, one at a time
# NOTE: Only need 1 file (txt) minimum
# for fn in os.listdir(seed_lists_folder):
  # print fn
script_path = os.path.dirname(os.path.realpath(__file__))
fn =  "seed.txt"
  
# Construct list file name
list_filename = ''.join([script_path, "/", fn])

print script_path
print list_filename

# Open list file and loop through each URL in seed list
# If URL is not empty, push to link_list[]
f=open(list_filename,'r')        
for url in f.readlines():
    if len(url) > 0:
        url = url.replace("\n", "")
        link_list.append(url)
f.close()

# Loop through all items in list
while True:
  base_url = link_list.pop()
  print "base_url", base_url
  # print "base url:", "http://" + base_url
  count += 1

    # Try connecting to base_url
    # try:
        
      # Extract HTML from url connection
      # tries = 0
      # html = ""
      
  try:
    # tries = 0
    html = ""
    # print "base url2:", base_url
    if base_url[0:4] == "www.":
      base_url = "http://" + base_url
    print base_url
    connection = urllib.urlopen(base_url) #"http://" + base_url) 
    html = connection.read()      
    # print len(html), base_url
    if len(html) > 0:                

        # Save html
        # print "..."
        # save_html(html, base_url, multiplier = 5)

        # Scrape html for links
      dom =  lxml.html.fromstring(html)

      # Look for links in this document 
      for link in dom.xpath('//a/@href'): 

        # Construct a URL
        temp_url = build_url(base_url, link)

        # Let's process this url
        if len(temp_url) > 0:

          # crawled.append(temp_url)
          log_url(temp_url)
          link_count += 1   

          # clear_output(wait=True)                        
          print len(link_list), link_count


  # except URLError, e:
    # print "URLError"
    # pass
  # except socket.timeout, e:
    # print "Socket.timeout"
    # pass
  except Exception, e:
    print "Exception: ", e
    # except Exception as e:
        # err_count += 1 
        # print err_count
    
    # Main list 'link_list' is empty, if there are
    # crawled links add to 
  if len(link_list)< 1: 
    # link_list = list(crawled)
    # crawled = []

    # if len(link_list) < 1:
    break

log.close()
print "Links collected: ", link_count
 
