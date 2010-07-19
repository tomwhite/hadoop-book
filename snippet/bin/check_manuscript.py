#!/usr/bin/env python

# Check that the expected (or actual) snippets are in the manuscript. E.g.
# bin/check_manuscript.py  ~/workspace/htdg2/ch11.xml expected/ch11/grunt/*

import sys

manuscript = open(sys.argv[1], 'r').read()

for snippet_file in sys.argv[2:]:
  index = manuscript.find(open(snippet_file, 'r').read())
  if index == -1:
    print "Snippet not found", snippet_file
    sys.exit(1)
