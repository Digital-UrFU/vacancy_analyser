#!/usr/bin/python3

import secrets
import hashlib
import base64

open("postgres.env", "a").close()
open("docker-compose.yml", "a").close()
open("hadoop_data/apache/apache.conf", "a").close()
open("hadoop_data/apache/apache_htpasswd", "a").close()
open("hadoop_data/etc_hadoop/httpfs-signature.secret", "a").close()

DOCKER_COMPOSE_TEMPLATE = open("docker-compose.yml.template", "r").read()
APACHE_CONF_TEMPLATE = open("hadoop_data/apache/apache.conf.template", "r").read()

if (open("postgres.env").read() or
    open("docker-compose.yml").read() or
    open("hadoop_data/apache/apache.conf").read() or
    open("hadoop_data/apache/apache_htpasswd").read() or
    open("hadoop_data/etc_hadoop/httpfs-signature.secret").read()):

    print("First time setup already done, exiting")
    print("To rerun it, execute 'rm postgres.env docker-compose.yml hadoop_data/apache/apache.conf hadoop_data/apache/apache_htpasswd hadoop_data/etc_hadoop/httpfs-signature.secret'")
    exit(1)


print("Welcome to the first time setup. Let us configure all passwords")
DOMAIN = input("Enter your domain name for HTTPS (or enter 'localhost' if the server has no name): ").strip()

if not DOMAIN:
    print("No domain, exiting")
    exit(1)

POSTGRES_USER = "vacancy"
POSTGRES_PASSWORD = secrets.token_urlsafe(12)
JUPYTER_PASS = secrets.token_urlsafe(12)
APACHE_PASS = secrets.token_urlsafe(12)
HTTPFS_SIGNATURE = secrets.token_urlsafe(21)

print()
print("Please write down this information somewhere!")
print(f"Jupyter at https://{DOMAIN}/, password {JUPYTER_PASS}")
print(f"Postgres: 'psql -h {DOMAIN} -U vacancy', password {POSTGRES_PASSWORD}")
print(f"HDFS web-interface at https://{DOMAIN}:4430, user hadoop, password {APACHE_PASS}")

postgres_env = f"""POSTGRES_USER={POSTGRES_USER}
POSTGRES_PASSWORD={POSTGRES_PASSWORD}
"""

JUPYTER_SALT = secrets.token_hex(6)
JUPYTER_CREDS = f"sha1:{JUPYTER_SALT}:{hashlib.sha1((JUPYTER_PASS+JUPYTER_SALT).encode()).hexdigest()}"

APACHE_CREDS = f"hadoop:{{SHA}}{base64.b64encode(hashlib.sha1(APACHE_PASS.encode()).digest()).decode()}"

if "{JUPYTER_CREDS}" not in DOCKER_COMPOSE_TEMPLATE:
    print("Warning: no {JUPYTER_CREDS} in docker_compose template, possibly broken file")

docker_compose = DOCKER_COMPOSE_TEMPLATE.replace("{JUPYTER_CREDS}", JUPYTER_CREDS, 1)

if "{DOMAIN}" not in APACHE_CONF_TEMPLATE:
    print("Warning: no {DOMAIN} in hadoop_data/apache/apache.conf.template, possibly broken file")

apache_conf = APACHE_CONF_TEMPLATE.replace("{DOMAIN}", DOMAIN)


# writing files
open("postgres.env", "w").write(postgres_env)
open("docker-compose.yml", "w").write(docker_compose)
open("hadoop_data/apache/apache.conf", "w").write(apache_conf)
open("hadoop_data/apache/apache_htpasswd", "w").write(APACHE_CREDS)
open("hadoop_data/etc_hadoop/httpfs-signature.secret", "w").write(HTTPFS_SIGNATURE)

print()
print("All done. Please save the passwords somewhere")
print("To run the system execute 'docker-compose up -d'")
