import requests
import time
import sys
import csv
import traceback
import os
import tempfile

from datetime import datetime

BASE_URL = "https://m.habr.com"
POST_URL = BASE_URL + "/ru/post"

BUCKET_SIZE = 100
MAX_ID = 1_000_000

TIMEOUT = 600

PROXIES = None

COLUMN_NAMES = ["id", "text"]

session = requests.session()

def log(*args, **kwargs):
    timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    print(timestamp, *args, **kwargs, file=sys.stderr, flush=True)



def add_article_to_csv(article_id, article_text, writer):
    writer.writerow({
        'id': article_id,
        'text': article_text
    })


try:
    os.mkdir("habr_data")
except FileExistsError:
    pass

os.chdir("habr_data")

for start_id in range(0, MAX_ID, BUCKET_SIZE):
    filename = f"{start_id}.csv"
    if os.path.exists(filename):
        log(f"File {filename} exists, continue")
        continue

    fd, tempname = tempfile.mkstemp(prefix=f"{filename}-unfinished-", dir="")
    print(tempname)
    os.chmod(tempname, 0o755)

    with open(fd, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=COLUMN_NAMES)
        writer.writeheader()

        for post_id in range(start_id, start_id+BUCKET_SIZE):
            log(f"Dumping post_id={post_id}")
            resp = session.get(POST_URL + f"/{post_id}/", proxies=PROXIES, timeout=TIMEOUT)
            if resp.status_code != 200:
                log(f"Failed to get {post_id}, skipping")
                continue

            article = resp.text
            add_article_to_csv(post_id, article, writer)

    os.rename(tempname, filename)
