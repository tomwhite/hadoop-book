#!/usr/bin/env python

import sys

# Change date to month and day
for line in sys.stdin:
  (station, date, temp) = line.strip().split("\t")
  print "%s\t%s\t%s" % (station, date[4:8], temp) 