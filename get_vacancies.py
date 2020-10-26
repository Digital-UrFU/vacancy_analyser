import requests
import time
import sys
import csv
import traceback

from datetime import datetime

BASE_URL = "https://api.hh.ru"
VACANCIES_URL = BASE_URL + "/vacancies"
EMPLOYER_URL = BASE_URL + "/employers"

MIN_DATE_DIFF = 60
MAX_YEARS_BACK = 5
MAX_YEARS_FWD = 5

session = requests.session()

def log(*args, **kwargs):
    timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    print(timestamp, *args, **kwargs, file=sys.stderr, flush=True)


def get_hh_vacancies(specialization="1", date_from:int=None, date_to:int=None, page=0):
    """Generator of vacancies, can return repeating ones due to api restrictions"""

    global session

    # log(f"get_hh_vacancies date_from={date_from}, date_to={date_to}")

    if date_from and date_to and date_to - date_from < MIN_DATE_DIFF:
        log(f"Time difference is too low: {date_from} {date_to}, skipping")
        return

    params = {
        "specialization": specialization,
        "per_page": 100,
        "page": page
    }

    if date_from:
        params["date_from"] = datetime.fromtimestamp(int(date_from)).isoformat()
    if date_to:
        params["date_to"] = datetime.fromtimestamp(int(date_to)).isoformat()

    result = session.get(VACANCIES_URL, params=params).json()

    if result["pages"] * result["per_page"] < result["found"]:
        SECONDS_IN_YEAR = 60*60*24*365.25
        if not date_from:
            date_from = time.time() - MAX_YEARS_BACK * SECONDS_IN_YEAR
        if not date_to:
            date_to = time.time() + MAX_YEARS_FWD * SECONDS_IN_YEAR

        date_middle = (date_from + date_to) / 2

        yield from get_hh_vacancies(specialization, date_from, date_middle)
        yield from get_hh_vacancies(specialization, date_middle, date_to)
        return

    yield from result["items"]

    if (page+1) < result["pages"]:
        yield from get_hh_vacancies(specialization, date_from, date_to, page+1)


def gen_all_hh_vacancy_ids(specialization="1"):
    used = set()
    for vacancy in get_hh_vacancies():
        if vacancy["id"] not in used:
            used.add(vacancy["id"])
            yield vacancy["id"]


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

    response = session.get(f"{EMPLOYER_URL}/{employer_id}")
    if response.status_code == 200:
        employer = response.json()
        return "\n".join(industry["name"] for industry in employer['industries'])
    else:
        log("Bad response code {response_employer.status_code} on getting employer industries")
        return []


def add_hh_vacancy_to_csv(vacancy: dict, writer):
    employer_industries = get_employer_industries(vacancy['employer'].get('id'))

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


with open('result.csv', 'w', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=COLUMN_NAMES)
    writer.writeheader()

    for pos, vacancy_id in enumerate(gen_all_hh_vacancy_ids()):
        log(f"Dumping pos={pos} vacancy_id={vacancy_id}")
        resp = session.get(VACANCIES_URL + f"/{vacancy_id}")
        if resp.status_code != 200:
            log(f"Failed to get {vacancy_id}, skipping")
            continue

        vacancy_obj = resp.json()
        add_hh_vacancy_to_csv(vacancy_obj, writer)
