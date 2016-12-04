import argparse
import csv
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import hashlib
from pymongo import MongoClient


class page_retrieval:
    def __init__(self):
        self.counter = 0
        self.dup_counter = 0

    def retrieval_controller(self, seed_file):
        client = MongoClient()
        db = client.cdsearchdb

        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

        f = open(seed_file)
        url_csv = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)

        print("Total records in test db: {0}".format(db.webpages.count()))

        for r in url_csv:
            if r:
                raw_url = r[0]
                hashed_url = hashlib.md5(raw_url.encode()).hexdigest()
                if self.dup_check(db, hashed_url):
                    success, html = self.retrieve_html(raw_url)
                    if success:
                        try:
                            self.mongodb_insert(db, hashed_url, raw_url, html)
                        except Exception:
                            pass
                else:
                    self.dup_counter += 1

        print("Total records in test db: {0}".format(db.webpages.count()))

    def dup_check(self, db, hashed_url):
        dups = False
        existing = db.webpages.find_one({"_id": hashed_url})
        if existing is None:
            dups = True
        return dups

    def retrieve_html(self, raw_url):
        valid_url = False
        html = "Empty"
        try:
            raw_response = requests.get(raw_url, verify=False, timeout=10)
            valid_url = raw_response.ok
            if valid_url:
                html = raw_response.text
        except Exception:
            pass
        return valid_url, html

    def mongodb_insert(self, db, hashed_url, raw_url, html):
        insert_result = db.webpages.insert_one(
            {
                "_id": hashed_url,
                "raw_url": raw_url,
                "html": html
            }
        )
        self.counter += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("file_path", nargs='?',
                        help="provide url seed file path")
    args = parser.parse_args()

    page_retievals = page_retrieval()
    page_retievals.retrieval_controller(args.file_path)
