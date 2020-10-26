#!/usr/bin/env python3

import sys
import os
import datetime
import subprocess
import re
import time
import traceback
import tempfile

DAYS_BETWEEN_DOWNLOADS = 7
RECHECK_EVERY_SEC = 60

DATA_DIR = "data"

RUN_CMD = ["python3", "-u", os.path.abspath("get_vacancies.py")]
MAX_RUN_SECS = 24 * 60 * 60

def log(*args, **kwargs):
    timestamp = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
    print(timestamp, *args, **kwargs, file=sys.stderr, flush=True)

def get_time_to_wait():
    DATE_RE = r"\d\d\d\d-\d\d-\d\d"
    prev_dirs = [d for d in os.listdir() if re.fullmatch(DATE_RE, d, re.ASCII)]

    for prev_dir in sorted(prev_dirs, reverse=True):
        dir_date = datetime.datetime.strptime(prev_dir, "%Y-%m-%d")

        if dir_date > datetime.datetime.now():
            log(f"Warning: dir date {dir_date} is from future, skipping")
            continue

        next_date = dir_date + datetime.timedelta(days=DAYS_BETWEEN_DOWNLOADS)

        time_to_wait = (next_date - datetime.datetime.now()).total_seconds()
        return max(0, time_to_wait)
    return 0


def run_once():
    dir_prefix = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")

    tempdir = tempfile.mkdtemp(prefix=f"{dir_prefix}-unfinished-", dir="")
    os.chmod(tempdir, 0o755)

    log_file = os.path.join(tempdir, "log.txt")
    log_file_pretty = os.path.join(DATA_DIR, log_file)

    log(f"Capturing stdout and stderr to {log_file_pretty}")

    with open(log_file, "wb") as f:
        process = subprocess.run(RUN_CMD, timeout=MAX_RUN_SECS, cwd=tempdir,
                                 stdout=f, stderr=subprocess.STDOUT)

        success = (process.returncode == 0)

        if success:
            log(f"Task successfully finished")
            os.rename(tempdir, dir_prefix)
        else:
            log(f"Bad return code: {process.returncode}, see log in {log_file_pretty}")


def loop():
    log(f"Starting the main loop")
    while True:
        try:
            wait_time = get_time_to_wait()
            if wait_time == 0:
                log(f"Launching task")
                run_once()
                time.sleep(RECHECK_EVERY_SEC)
            else:
                log(f"Next run in {int(wait_time)} secs")
                wait_time = min(wait_time, RECHECK_EVERY_SEC)
                time.sleep(wait_time)
        except Exception:
            log(traceback.format_exc())
            time.sleep(RECHECK_EVERY_SEC)


if __name__ == "__main__":
    os.chdir(DATA_DIR)
    loop()
