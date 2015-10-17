import sys
import os
import logging
import pg8000
import conf
import time


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

db_name = None


def execute(sqls):
    connection = pg8000.connect(host=conf.HOST, user='postgres', password='cz4031', database=db_name)
    cursor = connection.cursor()
    for sql in sqls.split(';')[:-1]:
        cursor.execute(sql.replace('%', '%%'))
    connection.commit()


def main():
    global db_name
    if len(sys.argv) < 3:
        print 'Please provide path to SQL file and db name'
        exit(1)
    input_path = sys.argv[1]
    db_name = sys.argv[2]
    real_path = os.path.abspath(input_path)
    # prepare.clean_db()
    log.info('Process file ' + real_path)
    with open(real_path, 'r') as f:
        text = f.read()
        queries = text.split('-'*10)
        for q in queries:
            print "Executing:", q

            start = time.time()
            execute(q)
            end = time.time()
            print "Time:", end-start
            for i in range(10):
                print


if __name__ == '__main__':
    main()