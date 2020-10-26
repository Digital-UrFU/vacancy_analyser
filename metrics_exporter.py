import os
import sys
import time
import traceback
import re
import socket

from datetime import date, datetime

import hdfs
import psycopg2
import psycopg2.extras

from hdfs import InsecureClient
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, REGISTRY

HOST = os.environ.get("POSTGRES_HOST", "db")
USER = os.environ.get("POSTGRES_USER", "vacancy")
PASSWORD = os.environ.get("POSTGRES_PASSWORD", "psql")
DB = os.environ.get("POSTGRES_DB", "vacancy")

PARQUET_FILE = "/vacancy.parquet"

DEFAULT_DATE = date(year=1970, month=1, day=1)

def log(*args, file=sys.stderr, **kwargs):
    timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    print(timestamp, *args, **kwargs, file=file, flush=True)

def get_file_names():
    DATE_RE = r"\d\d\d\d-\d\d-\d\d"
    DATA_DIR = "data/"

    return sorted(d for d in os.listdir(DATA_DIR) if re.fullmatch(DATE_RE, d, re.ASCII))
    

def get_file_dates():
    dirs = get_file_names()
    dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in dirs]

    return dates
    

def get_file_max_date():
    dates = get_file_dates()
    if not dates:
        return DEFAULT_DATE

    return max(dates)


def get_last_csv_size():
    DATA_DIR = "data/"
    FILENAME = "result.csv"
    try:
        dirs = get_file_names()
        if not dirs:
            return 0
        last_dir = max(dirs)
        
        return os.path.getsize(os.path.join(DATA_DIR, last_dir, FILENAME))
    except Exception as e:
        log(e)
        return 0


def get_db_max_date():
    try:
        conn = psycopg2.connect(dbname=DB, user=USER, password=PASSWORD, host=HOST)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("select max(added_at),max(updated_at),max(removed_at) from vacancy;")
            row = cursor.fetchone()
            if not row:
                return DEFAULT_DATE
        conn.close()
        dates = [d for d in row if d]
        if not dates:
            return DEFAULT_DATE
        return max(dates)
    except Exception as e:
        log(f"Exception {e} on get_db_max_date")
        return DEFAULT_DATE


def get_hdfs_max_date():
    SUCCESS_FILE = f"{PARQUET_FILE}/_SUCCESS"

    try:
        client = InsecureClient('http://namenode:9870', user='metrics')
        time_ts = client.status(SUCCESS_FILE)["modificationTime"] / 1000
        return date.fromtimestamp(time_ts)
    except Exception:
        log("Exception while trying to get parquet max date")
        log(traceback.format_exc())
        return DEFAULT_DATE


def get_now_date():
    return datetime.now().date()


def check_tcp_connection(host, port):
    TIMEOUT = 0.1
    try:
        socket.create_connection((host, port), timeout=TIMEOUT)
        return True
    except Exception as e:
        log(e)
        return False
        

class CustomCollector(object):
    def collect(self):
        c = GaugeMetricFamily("vacancy_lastdata", "Last vacancy data update in days from now", labels=["source"])

        file_date = get_file_max_date()
        db_date = get_db_max_date()
        hdfs_date = get_hdfs_max_date()
        now_date = get_now_date()

        if file_date != DEFAULT_DATE:
            c.add_metric(["file"], (now_date - file_date).days)
        if db_date != DEFAULT_DATE:
            c.add_metric(["db"], (now_date - db_date).days)
        if hdfs_date != DEFAULT_DATE:
            c.add_metric(["hdfs"], (now_date - hdfs_date).days)

        yield c

        c2 = GaugeMetricFamily("vacancy_services_up", "Service is up", labels=["service"])

        c2.add_metric(["datanode"], check_tcp_connection("datanode", 9864))
        c2.add_metric(["historyserver"], check_tcp_connection("historyserver", 8188))
        c2.add_metric(["namenode"], check_tcp_connection("namenode", 9000))
        c2.add_metric(["nodemanager"], check_tcp_connection("nodemanager", 8042))
        c2.add_metric(["resourcemanager"], check_tcp_connection("resourcemanager", 8088))
        c2.add_metric(["apache"], check_tcp_connection("apache", 443))
        c2.add_metric(["db"], check_tcp_connection("db", 5432))
        c2.add_metric(["jupyter"], check_tcp_connection("jupyter", 8888))

        yield c2

        c3 = CounterMetricFamily("vacancy_days_downloaded", "Number of days downloaded", labels=["service"])
        
        file_dates = get_file_dates()
        if file_dates:
            c3.add_metric(["file"], len(file_dates))
            yield c3
        
        yield GaugeMetricFamily("vacancy_last_csv_size", "The size of last CSV", value=get_last_csv_size())


REGISTRY.register(CustomCollector())
start_http_server(9144)


#print(f"{get_file_max_date()=}")
#print(f"{get_db_max_date()=}")
#print(f"{get_hdfs_max_date()=}")
#print(f"{get_now_date()=}")

while True:
    time.sleep(60)
