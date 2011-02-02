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

# Grupo 03

import zookeeper, threading, sys, time, base
from base import *

class ZooKeeperSemaphore(ZooKeeperBase):
  def __init__(self, semaphorename, hostname, port, initial_value = 0):
    ZooKeeperBase.__init__(self, hostname, port)
    try:
      zookeeper.create(self.handle,self.semaphorename,"semaphore top level", [ZOO_OPEN_ACL_UNSAFE],0)
      for x in xrange(
      print "Created new semaphore, OK"
    except zookeeper.NodeExistsException:
      print "Semaphore Already Exists"

  def signal(self,amount = 1):
    """
    Increases the signal by @amount
    """
    zookeeper.create(self.handle, self.semaphorename+"/item", val, [ZOO_OPEN_ACL_UNSAFE],zookeeper.SEQUENCE)

  def wait(self, amount = 1):
    """
    Decreases the signal by @amount and block call if semaphore is 0
    """
    def queue_watcher(handle,event,state,path):
      self.cv.acquire()
      self.cv.notify()
      self.cv.release()

    while True:
      self.cv.acquire()
      children = zookeeper.get_children(self.handle, self.semaphorename, queue_watcher)
      if len(children) == 0:
        time.sleep(0.1)
        continue

      for child in children:
        data = self.get_and_delete(self.semaphorename + "/" + child)
        if data != None:
          self.cv.release()
          return data
        # so the lock may be released several times on this for loop!?
        self.cv.wait()
        self.cv.release()

  def block_dequeue(self):
    """
    Similar to dequeue, but if the queue is empty, block until an item
    is added and successfully removed.
    """
    def queue_watcher(handle,event,state,path):
      self.cv.acquire()
      self.cv.notify()
      self.cv.release()
    while True:
      self.cv.acquire()
      children = sorted(zookeeper.get_children(self.handle, self.semaphorename, queue_watcher))
      for child in children:
        data = self.get_and_delete(self.semaphorename+"/"+children[0])
        if data != None:
          self.cv.release()
          return data
        self.cv.wait()
        self.cv.release()

# vim:tw=2:ts=2:et
