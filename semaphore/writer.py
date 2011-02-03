#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  Copyright (C) 2011 - Gustavo Serra Scalet <gsscalet@gmail.com>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

__AUTHOR__ = "David Kurka <david.kurka@gmail.com>"

# Grupo 03

import zookeeper, semaphore

MIN_ARGS = 1
__VERSION__ = 0.1

def writer(host, port, text="o gut realmente eh mto gutbobo"):
  """
  Connects to zookeeper and writes #TODO#
  """
  emptyBuffers = ZooKeeperSemaphore("emptybuffers", host, port, buffersize)
  fullBuffers = ZooKeeperSemaphore("fullbuffers", host, port, buffersize)
  try:
    writePt = 0
    for letter in text:
      #sleep random time
      #wait for avaiable resourcess
      emptyBuffers.wait()
      #write on shared memory
      shared[writePt] = letter #TODO: find out how to share a structure
      print "Writer: shared[%d] = %c\n" %writePt, %letter
      writePt = (writePt + 1) % buffersize
      #aloow reading
      fullBuffers.signal()
  except KeyboardInterrupt:
    pass
  emptyBuffers.__del__()
  fullBuffers.__del__()

if __name__ == "__main__":
  from sys import argv, exit
  from os import sep
  from optparse import OptionParser

  #options: -verbose -buffersize=5 -data_lenght=30 -data
  options = {
    # 'one_letter_option' : ['full_option_name',
      # "Help",
      # default_value],
    'H' : ['host',
      "Host to connect (default: localhost)",
      "localhost"],
  }

  options_list = ' '.join(["[-%s --%s]" % (o, options[o][0]) for o in options])
  desc = writer.__doc__.replace('  ','')
  parser = OptionParser("%%prog %s PORT" % options_list,
          description=desc,
          version="%%prog %s" % __VERSION__)

  for o in options:
    shorter = '-' + o
    longer = '--' + options[o][0]
    if type(options[o][2]) is bool:
      parser.add_option(shorter, longer, dest=o, help=options[o][1],
          action="store_true", default=options[o][2])
    elif type(options[o][2]) is str:
      parser.add_option(shorter, longer, dest=o, help=options[o][1],
          action="store", type="string", default=options[o][2])

  (opt, args) = parser.parse_args(argv)
  if len(args) < MIN_ARGS + 1:
    # not enough arguments
    print """ERROR: not enough arguments.
Try `%s --help' for more information""" % args[0].split(sep)[-1]
    exit(1)

  writer(opt.H, int(args[1]))

# vim:sw=2:ts=2:et
