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

import zookeeper, semaphore, time, random, os
# change to this directory
os.chdir(os.path.dirname(os.path.realpath(__file__)))

MIN_ARGS = 1
__VERSION__ = 0.1

def reader(host, buffersize, textsize, port):
  """
  Connects to zookeeper and reads
  """
  # create the semaphores, if they don't exist
  emptybuffers = semaphore.ZooKeeperSemaphore("emptybuffers", host, port, buffersize)
  fullbuffers = semaphore.ZooKeeperSemaphore("fullbuffers", host, port)
  try:
    readPt = 0
    for letter in xrange(textsize):
      # waiting for writer
      fullbuffers.wait()

      # read from shared memory and remove it
      filename = semaphore.BUFFER_PATTERN % readPt
      f = open(filename, 'r')
      data = f.read()
      f.close()
      os.remove(filename)
      print "Reader: %s = %c" % (filename, data)
      readPt = (readPt + 1) % buffersize

      # allow writing
      emptybuffers.signal()
      # sleep random time
      time.sleep(random.random())
  except KeyboardInterrupt:
    pass
  emptybuffers.__del__()
  fullbuffers.__del__()

if __name__ == "__main__":
  from sys import argv, exit
  from os import sep
  from optparse import OptionParser

  options = {
    # 'one_letter_option' : ['full_option_name',
      # "Help",
      # default_value],
    'H' : ['host',
      "Host to connect (default: localhost)",
      "localhost"],
    'b' : ['buffersize',
      "Space avaiable for writing (default: 5)",
      5],
    's' : ['data_length',
      "Length of complete message (default: 30)",
      30],
  }

  options_list = ' '.join(["[-%s --%s]" % (o, options[o][0]) for o in options])
  desc = reader.__doc__.replace('  ','')
  parser = OptionParser("%%prog %s PORT" % options_list,
          description=desc,
          version="%%prog %s" % __VERSION__)

  for o in options:
      shorter = '-' + o
      longer = '--' + options[o][0]
      if type(options[o][2]) is bool:
          parser.add_option(shorter, longer, dest=o, help=options[o][1],
              action="store_true", default=options[o][2])
      elif type(options[o][2]) is int:
          parser.add_option(shorter, longer, dest=o, help=options[o][1],
              action="store", type="int", default=options[o][2])
      elif type(options[o][2]) is str:
          parser.add_option(shorter, longer, dest=o, help=options[o][1],
              action="store", type="string", default=options[o][2])

  (opt, args) = parser.parse_args(argv)
  if len(args) < MIN_ARGS + 1:
      # not enough arguments
      print """ERROR: not enough arguments.
Try `%s --help' for more information""" % args[0].split(sep)[-1]
      exit(1)

  reader(opt.H, opt.b, opt.s, int(args[1]))

# vim:sw=2:ts=2:et
