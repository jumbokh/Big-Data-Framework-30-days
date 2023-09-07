#!/usr/bin/env python
import sys

curr_word = None
curr_count = 0

for line in sys.stdin:

    line = line.strip()
    word, count = line.split('\t')
    count = int(count)

    if curr_word != word:
        if curr_word : 
            print(f"{curr_word}\t{curr_count}")
        curr_word = word
        curr_count = 0
    
    curr_count += count

if curr_word:
    print(f"{curr_word}\t{curr_count}")