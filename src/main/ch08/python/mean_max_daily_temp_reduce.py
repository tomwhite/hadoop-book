#!/usr/bin/env python

import sys

(last_key, count, sum) = (None, 0, 0)
for line in sys.stdin:
  (station, month_day, temp) = line.strip().split("\t")
  key = "%s\t%s" % (station, month_day)
  if last_key and last_key != key:
    print "%s\t%s" % (last_key, sum / count)
    (last_key, count, sum) = (key, 1, int(temp))
  else:
    (last_key, count, sum) = (key, count + 1, sum + int(temp))

if last_key:
  print "%s\t%s" % (last_key, sum / count)