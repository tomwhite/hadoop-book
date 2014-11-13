#!/usr/bin/env python

import re
import sys

for line in sys.stdin:
  val = line.strip()
  (usaf, wban, date, temp, q) = (val[4:10], val[10:15], val[15:23],
                                 int(val[87:92]), val[92:93])
  if (temp != 9999 and re.match("[01459]", q)):
    print "%s-%s\t%s\t%s" % (usaf, wban, date, temp)