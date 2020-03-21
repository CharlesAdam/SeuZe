import sys
import logging
import rds_config
import psycopg2

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

    item_count = 0

    with conn.cursor() as cur:
        cur.execute(
            '''INSERT INTO ft_report_brazil_city_dairy(
                datasource,
                city,
                state,
                uid,
                suspects,
                refuses,
                cases,
                casesnew,
                deaths,
                deathsnew,
                comments,
                broadcast
            ) values (
                'S3',
                'SP',
                'SP',
                1,
                0,
                0,
                0,
                1,
                1,
                1,
                'teste comentario',
                B'1'
                )''')
        conn.commit()
        cur.execute("select * from ft_report_brazil_city_dairy")
        for row in cur:
            item_count += 1
            logger.info(row)
            print(row)
    conn.commit()

    return "Added %d items from RDS PostgreSQL table" %(item_count)

lambda_handler()
