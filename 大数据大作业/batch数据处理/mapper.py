#!/usr/bin/env python
import sys
def read_input(file):
    for line in file:
        yield line.split()

def main(separator=' '):
    data = read_input(sys.stdin)
    for words in data:
       print "%s%s%s" % (words[3], separator, words[1])

if __name__ == "__main__":
    main()
