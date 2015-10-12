import sys
import logging
import os
import prepare
import item_processor


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


def get_tag(line):
    return line.split(' ')[0][1:]


def closing_tag(tag_name):
    return '</' + tag_name + '>'


def process_item(item_xml):
    log.debug('Item: ' + item_xml)
    item_processor.consume(item_xml)


def process_file(file_path):

    skip_count = 3
    current_tag = None
    read_buffer = ""

    with open(file_path, 'r') as f:
        for line in f:
            # Skip first few lines
            if skip_count > 0:
                skip_count -= 1
                continue

            # Remove newline from string end.
            if line[-1] == '\n':
                line = line[:-1]

            # Skip empty line
            if len(line) == 0:
                continue

            # Terminate condition
            if line == closing_tag('dblp'):
                break

            read_buffer += line

            if current_tag is None:
                current_tag = get_tag(line)
            elif line == closing_tag(current_tag):
                process_item(read_buffer)
                read_buffer = ""
                current_tag = None


def main():
    if len(sys.argv) < 2:
        print 'Please provide path to xml data file.'
        exit(1)
    input_path = sys.argv[1]
    real_path = os.path.abspath(input_path)
    prepare.clean_db()
    print 'Process file ' + real_path
    process_file(real_path)


if __name__ == '__main__':
    main()