import sys
import logging
import rds_config
import psycopg2
import json
from types import SimpleNamespace as Namespace

#rds settings
rds_host  = rds_config.rds_host
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = psycopg2.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=5)
except psycopg2.Error as e:
    logger.error("ERROR: Unexpected error: Could not connect to PostgreSQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS PostgreSQL instance succeeded")
def lambda_handler():
    """
    This function fetches content from PostgreSQL RDS instance
    """
    payload = '''{"count":291,"next":null,"previous":null,"results":[{"city":"Laranjal do Jarí","confirmed":0,"date":"2020-03-20","deaths":null,"discarded":null,"notes":null,"notified":null,"place_type":"city","source_url":"https://www.portal.ap.gov.br/noticia/2003/boletim-informativo-covid-19-amapa-20-de-marco-de-2020","state":"AP","suspect":2},{"city":"Macapá","confirmed":1,"date":"2020-03-20","deaths":null,"discarded":null,"notes":null,"notified":null,"place_type":"city","source_url":"https://www.portal.ap.gov.br/noticia/2003/coronavirus-primeiro-caso-e-confirmado-no-amapa","state":"AP","suspect":21},{"city":"Pedra Branca do Amapari","confirmed":0,"date":"2020-03-20","deaths":null,"discarded":null,"notes":null,"notified":null,"place_type":"city","source_url":"https://www.portal.ap.gov.br/noticia/2003/boletim-informativo-covid-19-amapa-20-de-marco-de-2020","state":"AP","suspect":1}]}'''
    report = json.loads(payload, object_hook=lambda d: Namespace(**d))
    item_count = 0
    with conn.cursor() as cur:
        for case in report.results:
            if(case.place_type == "city"):
                add_to_table(cur, case)
    

    return "Added %d items from RDS PostgreSQL table" %(item_count)


def add_to_table(cur, report):
    data = (report.city, report.state, report.deaths, report.discarded, report.confirmed, report.suspect, report.notes)
    cur.execute(
            '''INSERT INTO ft_report_brazil_city_dairy(
                datasource,
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
                %s
                )''', data)
    conn.commit()
    cur.execute("select * from ft_report_brazil_city_dairy")
    for row in cur:
        logger.info(row)
        print(row)
    conn.commit()


lambda_handler()
