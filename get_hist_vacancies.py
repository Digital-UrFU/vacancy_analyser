import requests
import time
import sys
import csv
import traceback
import os
import tempfile

from datetime import datetime

BASE_URL = "https://api.hh.ru"
VACANCIES_URL = BASE_URL + "/vacancies"
EMPLOYER_URL = BASE_URL + "/employers"

BUCKET_SIZE = 10000
MAX_ID = 40_000_000

TIMEOUT = 600

PROXIES = None
PAUSE = 1

session = requests.session()

def log(*args, **kwargs):
    timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    print(timestamp, *args, **kwargs, file=sys.stderr, flush=True)


COLUMN_NAMES = [
    'id',
    'description',
    'key_skills',
    'schedule_id',
    'schedule_name',
    'accept_handicapped',
    'accept_kids',
    'experience_id',
    'experience_name',
    'specializations',
    'contacts',
    'billing_type_id',
    'billing_type_name',
    'allow_messages',
    'premium',
    'driver_license_types',
    'accept_incomplete_resumes',

    'employer_id',
    'employer_name',
    'employer_vacancies_url',
    'employer_trusted',
    'employer_alternate_url',
    'employer_industries',
    'response_letter_required',
    'type_id',
    'type_name',
    'has_test',
    'response_url',
    'test_required',

    'salary_from',
    'salary_to',
    'salary_gross',
    'salary_currency',
    'archived',
    'name',
    'insider_interview',
    'area_id',
    'area_name',
    'area_url',
    'created_at',
    'published_at',

    'address_city',
    'address_street',
    'address_building',
    'address_description',
    'address_lat',
    'address_lng',
    'alternate_url',
    'apply_alternate_url',
    'code',
    'department_id',
    'department_name',
    'employment_id',
    'employment_name'
]


def get_employer_industries(employer_id=None):
    global session

    if not employer_id:
        return None

    response = session.get(f"{EMPLOYER_URL}/{employer_id}", proxies=PROXIES, timeout=TIMEOUT)
    if response.status_code == 200:
        employer = response.json()
        return "\n".join(industry["name"] for industry in employer['industries'])
    else:
        log("Bad response code {response_employer.status_code} on getting employer industries")
        return []


def add_hh_vacancy_to_csv(vacancy: dict, writer):
    employer_industries = get_employer_industries(vacancy['employer'].get('id'))

    it_vacancy = any(s['id'].split(".", 1)[0] == "1" for s in vacancy["specializations"])
    if not it_vacancy:
        log(f"Vacancy {vacancy['id']} is not an IT vacancy, skipping")
        return

    specializations = (f"{s['id']} {s['name']} {s['profarea_id']} {s['profarea_name']}"
                      for s in vacancy["specializations"])

    contacts = []
    if vacancy['contacts'] != None:
        if vacancy['contacts']['name']:
            contacts.append(vacancy['contacts']['name'])
        if vacancy['contacts']['email']:
            contacts.append(vacancy['contacts']['email'])
        for p in vacancy['contacts']['phones']:
            contacts.append(f"{p['country']} {p['city']} {p['number']} {p['comment']}")

    writer.writerow({
        'id': vacancy['id'],
        'description': vacancy['description'],
        'key_skills': "\n".join(skill['name'] for skill in vacancy['key_skills']),
        'schedule_id': vacancy['schedule']["id"] if vacancy['schedule'] else None,
        'schedule_name': vacancy['schedule']["name"] if vacancy['schedule'] else None,
        'accept_handicapped': vacancy['accept_handicapped'],
        'accept_kids': vacancy['accept_kids'],
        'experience_id': vacancy['experience']['id'] if vacancy['experience'] else None,
        'experience_name': vacancy['experience']['name'] if vacancy['experience'] else None,
        'specializations': "\n".join(specializations),
        'contacts': "\n".join(contacts),
        'billing_type_id': vacancy['billing_type']['id'] if vacancy['billing_type'] else None,
        'billing_type_name': vacancy['billing_type']['name'] if vacancy['billing_type'] else None,
        'allow_messages': vacancy['allow_messages'],
        'premium': vacancy['premium'],
        'driver_license_types': "\n".join(t['id'] for t in vacancy['driver_license_types']),
        'accept_incomplete_resumes': vacancy['accept_incomplete_resumes'],
        'employer_id': vacancy['employer'].get("id"),
        'employer_name': vacancy['employer'].get("name"),
        'employer_vacancies_url': vacancy['employer'].get("vacancies_url"),
        'employer_trusted': vacancy['employer'].get("trusted"),
        'employer_alternate_url': vacancy['employer'].get("alternate_url"),
        'employer_industries': employer_industries,
        'response_letter_required': vacancy['response_letter_required'],
        'type_id': vacancy['type']['id'] if vacancy['type'] else None,
        'type_name': vacancy['type']['name'] if vacancy['type'] else None,
        'has_test': vacancy['has_test'],
        'response_url': vacancy['response_url'],
        'test_required': vacancy['test']['required'] if vacancy['test'] else None,
        'salary_from': vacancy['salary']['from'] if vacancy['salary'] else None,
        'salary_to': vacancy['salary']['to'] if vacancy['salary'] else None,
        'salary_gross': vacancy['salary']['gross'] if vacancy['salary'] else None,
        'salary_currency': vacancy['salary']['currency'] if vacancy['salary'] else None,
        'archived': vacancy['archived'],
        'name': vacancy['name'],
        'insider_interview': vacancy['insider_interview'],
        'area_id': vacancy['area']['id'] if vacancy['area'] else None,
        'area_name': vacancy['area']['name'] if vacancy['area'] else None,
        'area_url': vacancy['area']['url'] if vacancy['area'] else None,
        'created_at': vacancy['created_at'],
        'published_at': vacancy['published_at'],
        'address_city': vacancy['address']['city'] if vacancy['address'] else None,
        'address_street': vacancy['address']['street'] if vacancy['address'] else None,
        'address_building': vacancy['address']['building'] if vacancy['address'] else None,
        'address_description': vacancy['address']['description'] if vacancy['address'] else None,
        'address_lat': vacancy['address']['lat'] if vacancy['address'] else None,
        'address_lng': vacancy['address']['lng'] if vacancy['address'] else None,
        'alternate_url': vacancy['alternate_url'],
        'apply_alternate_url': vacancy['apply_alternate_url'],
        'code': vacancy['code'],
        'department_id': vacancy['department']['id'] if vacancy['department'] else None,
        'department_name': vacancy['department']['name'] if vacancy['department'] else None,
        'employment_id': vacancy['employment']['id'] if vacancy['employment'] else None,
        'employment_name': vacancy['employment']['name'] if vacancy['employment'] else None
    })


try:
    os.mkdir("hist_data")
except FileExistsError:
    pass

os.chdir("hist_data")

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

        for vacancy_id in range(start_id, start_id+BUCKET_SIZE):
            log(f"Dumping vacancy_id={vacancy_id}")
            resp = session.get(VACANCIES_URL + f"/{vacancy_id}", proxies=PROXIES, timeout=TIMEOUT)
            if resp.status_code != 200:
                log(f"Failed to get {vacancy_id}, skipping")
                continue

            vacancy_obj = resp.json()
            add_hh_vacancy_to_csv(vacancy_obj, writer)
            time.sleep(PAUSE)

    os.rename(tempname, filename)
