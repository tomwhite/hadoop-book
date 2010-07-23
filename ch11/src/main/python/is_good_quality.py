#!/usr/bin/env python

import re
import string
import sys

for line in sys.stdin:
  (year, temp, q) = string.strip().split(line)
  if (temp != "9999" and re.match("[01459]", q)):
    print "%s\t%s" % (year, temp)
