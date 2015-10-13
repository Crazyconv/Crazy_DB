import logging
from lxml import etree
import helper
import collector

log = logging.getLogger(__name__)


def consume(item):
    package = {}
    root = etree.fromstring(item, parser=etree.XMLParser(recover=True))
    log.debug("Processing " + str(root))

    # Initial process
    package['class'] = root.tag
    package['pubkey'] = root.get('key')
    package['authors'] = []
    for child in root:
        if child.tag == 'author':
            package['authors'] += [child.text]
        else:
            package[child.tag] = get_text_from_node(child)
    package['authors'] = list(set(package['authors']))

    # Post process
    package['type'] = package['pubkey'].split('/')[0]
    package['year'] = int(package['year'])
    package['month'] = int(root.get('mdate').split('-')[1])
    if 'pages' in package and package['pages'] is not None:
        package['total_page'] = get_total_page(package['pages'])
    else:
        package['total_page'] = get_total_page('')

    # if 'volume' in package:
    #     package['volume'] = int(package['volume'])
    # if 'number' in package:
    #     package['number'] = int(package['number'])

    # Collect
    collector.collect(package)


def get_text_from_node(node):
    """
    Get text recursively from node's text and children.
    """
    return " ".join([t.strip() for t in node.itertext()])


def get_total_page(pages):
    return helper.get_total_page(pages)

