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

__AUTHOR__ = "Gustavo Serra Scalet <gsscalet@gmail.com>"
BUFFER_PATTERN = "buffer%%05d.dat"
_CHILD_KEY = "item"

# Grupo 03

import zookeeper, threading, sys, time, base
from base import *

class ZooKeeperSemaphore(ZooKeeperBase):
  def __init__(self, semaphorename, hostname, port, initial_value = 0):
    ZooKeeperBase.__init__(self, hostname, port)

    self.semaphore_name = "/" + semaphorename
    self.child_name = "%s/%s" % (self.semaphore_name, _CHILD_KEY)
    try:
      zookeeper.create(self.handle,self.semaphore_name,"semaphore top level", [ZOO_OPEN_ACL_UNSAFE],0)
      self.signal(initial_value)
      print "Created new semaphore, OK"
    except zookeeper.NodeExistsException:
      print "Semaphore Already Exists"

  def __del__(self):
    # clear the semaphore
    for x in xrange(self.getValue()):
      self.wait()
    self.get_and_delete(self.semaphore_name)

    print "freeing resources"
    ZooKeeperBase.__del__(self)

  def signal(self,amount = 1):
    """
    Increases the signal by @amount and returns a unique monotonically
    increasing sequence number related to the order of signal() calls
    """
    zpath = None
    for x in xrange(amount):
      zpath = zookeeper.create(self.handle, self.child_name, "foo", [ZOO_OPEN_ACL_UNSAFE],zookeeper.SEQUENCE)

    if zpath:
      return int(zpath.replace(self.child_name, ''))

  def wait(self, amount = 1):
    """
    Decreases the signal by @amount and block call if semaphore is 0
    """
    while True:
      children = zookeeper.get_children(self.handle, self.semaphore_name, self.__queueWatcher__)
      if len(children) == 0:
        time.sleep(0.1)
        continue

      for child in children:
        data = self.get_and_delete("%s/%s" % (self.semaphore_name, child))
        if data != None:
          return data

  def getValue(self):
    """
    Returns the how many times (signal() - wait()) were called
    """
    return len(zookeeper.get_children(self.handle, self.semaphore_name, self.__queueWatcher__))

if __name__ == "__main__":
  semaphore = ZooKeeperSemaphore('test', 'localhost', 32122)
  print "Semaphore initialized with %d value" % semaphore.getValue()

  print "Signalizing..."
  semaphore.signal()
  print "Semaphore with %d value" % semaphore.getValue()

  print "Waiting..."
  semaphore.wait()
  print "Semaphore with %d value" % semaphore.getValue()

  print "Waiting..."
  semaphore.wait()
  print "Semaphore with %d value" % semaphore.getValue()

  semaphore.__del__()

# vim:sw=2:ts=2:et
