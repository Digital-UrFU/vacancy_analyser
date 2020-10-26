#!/bin/bash

touch postgres.env
exec docker-compose -f docker-compose.yml.template run --rm vacancy_downloader python3 first_time_setup.py
