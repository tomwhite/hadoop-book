#!/usr/bin/env python

import re
import sys

filename = sys.argv[1]
output_dir = sys.argv[2]

fragments = {}
active_fragments = set([])

for line in open(filename, 'r'):
  m = re.match(r".*#\s+vv\s+([^<]*);", line)
  if m:
    active_fragments.add(m.group(1).strip())
    continue
  m = re.match(r".*#\s+\^\^\s+([^<]*);", line)
  if m:
    active_fragments.remove(m.group(1).strip())
    continue
  for fragment in active_fragments:
   fragments[fragment] = fragments.get(fragment, '') + line
   
for fragment in fragments:
  file = open(output_dir + "/" + fragment + ".xml", 'w')
  file.write('<screen format="linespecific">' + fragments[fragment].strip() + "</screen>" )
