#!/usr/bin/env python

# Check that the expected (or actual) snippets are in the manuscript. E.g.
# bin/check_manuscript.py  ~/book-workspace/htdg-git/ch16-pig.xml expected/ch16-pig/grunt/*

import sys

manuscript = open(sys.argv[1], 'r').read()

for snippet_file in sys.argv[2:]:
  lines = open(snippet_file, 'r').readlines()
  if lines[0].startswith("<!--"):
    doc = "".join(lines[1:]) # remove first line if a comment
  else:
    doc = "".join(lines[0:])
  snippet = doc.strip()
  index = manuscript.find(snippet)
  if index == -1:
    print "Snippet not found", snippet_file
  #else:
  #  print "Snippet found", snippet_file

