#!/usr/bin/env python

import sys

(last_key, max_val) = (None, 0)
for line in sys.stdin:
  (station, date, temp) = line.strip().split("\t")
  key = "%s\t%s" % (station, date)
  if last_key and last_key != key:
    print "%s\t%s" % (last_key, max_val)
    (last_key, max_val) = (key, int(temp))
  else:
    (last_key, max_val) = (key, max(max_val, int(temp)))

if last_key:
  print "%s\t%s" % (last_key, max_val)