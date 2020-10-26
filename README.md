# Vacancy Analyser
The tool-set to analyse the vacancy market in Russia

## Starting Up

1. `apt update && apt install -y docker-compose`
2. `git clone https://github.com/Digital-UrFU/vacancy_analyser.git && cd vacancy_analyser`
3. `./first_time_setup.sh`
4. `docker-compose up -d`
5. `docker-compose restart apache` # do it once to for a good SSL certificate

## Services

1. Jupyter with pyspark is on https://yourhost/
2. Postgres is on psql -h yourhost -U vacancy
3. HDFS web-interface is on https://yourhost:4430/

## The Working Scheme

1. Actual vacancies are periodicaly downloaded to csv
2. Special service puts new vacanies to postgres and marks deleted ones
3. Another service gets vacancies from postgres and puts them to HDFS in parquet format to use with PySpark

## Data

1. ./data # the vacancies from hh.ru in csv format, updated every week
2. ./hist_data # the historical vacancies from hh.ru, constantly downloaded with low rate
3. ./habr_data # articles from habr.com
