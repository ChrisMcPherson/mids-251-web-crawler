import argparse
import requests
from bs4 import BeautifulSoup

class  webCrawler:

    def __init__(self):
        self.defaultURL = "https://www.akronchildrens.org/cgi-bin/providers/find_a_provider.pl?myq=a&all=1"

    def getURL(self, page):
            start_link = page.find("a href")
            if start_link == -1:
                return None, 0
            start_quote = page.find('"', start_link)
            end_quote = page.find('"', start_quote + 1)
            url = page[start_quote + 1: end_quote]
            return url, end_quote

    def processPage(self, seedURL):
        response = requests.get(seedURL)
        page = str(BeautifulSoup(response.content))
        count = 0
        while True:

            url, n = self.getURL(page)
            page = page[n:]
            if count == 1000000:
                break
            #if url[0] != "h":
            #    continue
            if url:
                if url[0] == "/":
                    url = "{0}{1}".format(seedURL, url)
                    
                print(url)
                count = count + 1
                #self.processPage(url) # Recurssive call! This will break things... but it's just a test...in reality we'd likely use a db (graph db?) to store position and iterations
            else:
                print(count)
                break


    def initializeDefaultCrawler(self):
        self.processPage(self.defaultURL)

    def initializeCrawler(self, userURL):
        self.processPage(userURL)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("userURL", nargs='?',
                        help="provide a url to crawl")
    args = parser.parse_args()

    if args.userURL:
        crawler = webCrawler()
        crawler.initializeCrawler(args.userURL)
    else:
        crawler = webCrawler()
        crawler.initializeDefaultCrawler()

    