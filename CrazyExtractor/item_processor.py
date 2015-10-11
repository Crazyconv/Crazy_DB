import logging
from lxml import etree
import pg8000

log = logging.getLogger(__name__)

PUBLICATION_COLUMNS = ['pubkey', 'title', 'year', 'type', 'total_page']
EXTRA_COLUMNS = {
    'article': ['journal', 'month', 'volume', 'number'],
    'book':    ['publisher', 'isbn'],
    'incollection': ['booktitle', 'publisher', 'incollection'],
    'inproceedings': ['booktitle', 'editor']
}


def consume(item):
    package = {}
    root = etree.fromstring(item, parser=etree.XMLParser(recover=True))

    # Initial process
    package['class'] = root.tag
    package['pubkey'] = root.get('key')
    package['authors'] = []
    for child in root:
        if child.tag == 'author':
            package['authors'] += [child.text]
        else:
            package[child.tag] = child.text
    print package

    # Post process
    package['type'] = package['pubkey'].split('/')[0]
    package['year'] = int(package['year'])
    package['month'] = int(root.get('mdate').split('-')[1])
    package['total_page'] = get_total_page(package['pages'])

    if 'volume' in package:
        package['volume'] = int(package['volume'])
    if 'number' in package:
        package['number'] = int(package['number'])

    # Store
    store(package)


def get_total_page(pages):
    return 10


def sql_save_value(val):
    if isinstance(val, int):
        return str(val)
    else:
        return '\'' + val + '\''


def get_value_list(keys, package):
    return ','.join([sql_save_value(package[key]) for key in keys])


def pubid_from_pubkey(pubkey):
    return "(SELECT pubid FROM publication WHERE pubkey = '%s')" % (pubkey, )


def aid_from_name(name):
    return "(SELECT aid FROM author WHERE name = '%s')" % (name, )


def store(package):
    sqls = []
    post_sqls = []

    for author in package['authors']:
        sql_author = "INSERT INTO author (name) SELECT '%s' " \
                     "WHERE NOT EXISTS (SELECT aid FROM author WHERE name = '%s');" % (author, author)
        sqls.append(sql_author)
        sql_pub_author = "INSERT INTO pub_author (pubid, aid) VALUES (%s, %s);"\
                         % (pubid_from_pubkey(package['pubkey']), aid_from_name(author))
        post_sqls.append(sql_pub_author)

    pub_columns = [c for c in PUBLICATION_COLUMNS if c in package.keys()]
    sql_pub = 'INSERT INTO publication (%s) VALUES (%s);'\
              % (','.join(pub_columns), get_value_list(pub_columns, package))
    sqls.append(sql_pub)
    if package['class'] in EXTRA_COLUMNS.keys():
        extra_columns = [c for c in EXTRA_COLUMNS[package['class']] if c in package.keys()]
        sql_extra = 'INSERT INTO %s (%s) VALUES (%s);'\
                    % (package['class'],
                       ','.join(['pubid'] + extra_columns),
                       pubid_from_pubkey(package['pubkey']) + ',' + get_value_list(extra_columns, package))
        sqls.append(sql_extra)
    sqls = sqls + post_sqls
    print '\n'.join(sqls)
    execute(sqls)


def execute(sqls):
    connection = pg8000.connect(host='128.199.206.253', user='gxz', password='cz4031', database='dblp')
    cursor = connection.cursor()
    for sql in sqls:
        cursor.execute(sql)
    connection.commit()


