import os
import pg8000
import conf
import logging

SQL_FILE = os.path.abspath(os.path.join(__file__, '../../CrazyPostgres/tables.sql'))
log = logging.getLogger(__name__)


def clean_db():
    log.debug('Cleaning database.')
    with open(SQL_FILE, 'r') as f:
        sql_text = f.read()
        sqls = [s.strip() for s in sql_text.split(';') if len(s.strip()) > 0]
        connection = pg8000.connect(host=conf.HOST, user='gxz', password='cz4031', database='dblp')
        cursor = connection.cursor()
        for sql in sqls:
            cursor.execute(sql.replace('%', '%%'))
        connection.commit()
        log.debug("Done preparing.")