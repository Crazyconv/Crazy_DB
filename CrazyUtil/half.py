import sys
import os

def main():
    if len(sys.argv) < 2:
        print 'Please provide path to SQL file.'
        exit(1)
    input_path = sys.argv[1]
    real_path = os.path.abspath(input_path)

    block = ""
    count = 0
    with open(real_path, 'r') as f:
        for line in f:
            if line == '\n':
                if count == 0:
                    print block
                    count = 1
                else:
                    count = 0
                block = ""
            else:
                block += line

if __name__ == '__main__':
    main()