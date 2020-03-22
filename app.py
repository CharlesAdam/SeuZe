import sys
import logging
import rds_config
import psycopg2
import json
import urllib.parse
import boto3
import http
from types import SimpleNamespace as Namespace

#rds settings
rds_host  = rds_config.rds_host
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
conn = open_connection()

try:
    conn = psycopg2.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=5)
except psycopg2.Error as e:
    logger.error("ERROR: Unexpected error: Could not connect to PostgreSQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS PostgreSQL instance succeeded")
def lambda_handler(event, context):
    """
    This function fetches content from PostgreSQL RDS instance
    """

    bucket = fetch_bucket(event)
    key = fetch_key(event)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        parse_reponse(response)
    except Exception as e:
        print(e)
        logger.error(e)
    #return "Added %d items from RDS PostgreSQL table" %(item_count)

def fetch_bucket(event):
    return event['Records'][0]['s3']['bucket']['name']

def fetch_key(event):
    return urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

def open_connection():
    try:
        conn = psycopg2.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=5)
        logger.info("SUCCESS: Connection to RDS PostgreSQL instance succeeded")
        return conn
    except psycopg2.Error as e:
        logger.error("ERROR: Unexpected error: Could not connect to PostgreSQL instance.")
        logger.error(e)
        sys.exit()


def parse_reponse(response):
    payload = response['Body'].read()
    data = json.loads(payload, object_hook=lambda d: Namespace(**d))
    with conn.cursor() as cursor:
        for case in data.results:
            if(case.place_type == "city"):
                add_to_table(cursor, case)


def add_to_table(cursor, report):
    'This function adds a report to the table'
    data = (report.date, report.city, report.state, report.deaths, report.discarded, report.confirmed, report.suspect, report.notes)
    cursor.execute(
            '''INSERT INTO ft_report_brazil_city_dairy(
                datasource,
                date,
                city,
                state,
                deaths,
                refuses,
                cases,
                suspects,
                comments
            ) values (
                'brasilio-bucket',
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
                )''', data)
    conn.commit()