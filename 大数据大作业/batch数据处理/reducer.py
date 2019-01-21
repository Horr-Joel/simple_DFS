#!/usr/bin/env python

import sys


if __name__ == "__main__":

    '''
    dataset = []
    for line in sys.stdin:
        dataset.append(line.rstrip())

    news_dataset = list(set(dataset))
    news_dataset.sort(key=dataset.index)
    '''
    current_line = None

    for line in sys.stdin:
        line = line.rstrip()

        if line != current_line:
            if current_line:
                current_value, current_topic = current_line.split()
                value, topic = line.split()

                if (current_topic != topic) and (current_value == value):
                    print value

            current_line = line
