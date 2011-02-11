# Copyright (c) 2010, Henry Robinson
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the <organization> nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import zookeeper, threading, sys, time
ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"};

class ZooKeeperBase(object):
  def __init__(self, hostname, port):
    self.connected = False
    zookeeper.set_log_stream(open("/dev/null"))
    self.cv = threading.Condition()
    def watcher(handle,type,state,path):
      print "Connected"
      self.cv.acquire()
      self.connected = True
      self.cv.notify()
      self.cv.release()

    self.cv.acquire()
    connection = "%s:%d" % (hostname, port)
    self.handle = zookeeper.init(connection, watcher, 10000)
    self.cv.wait(10.0)
    if not self.connected:
      print "Connection to ZooKeeper cluster timed out - is a server running on localhost:%d?" % port
      sys.exit()
    self.cv.release()

    if not self.connected:
      print "Connection to ZooKeeper cluster timed out - is a server running on %s?" % connection
      sys.exit()

  def __del__(self):
    zookeeper.close(self.handle)
    print "Zookeeper handle closed and resources freed."

  def _blocker_watcher(self,handle,type,state,path):
    self.cv.acquire()
    self.cv.notify()
    self.cv.release()


  def get_and_delete(self,node):
    """
    Atomic get-and-delete operation. Returns None on failure.
    """
    try:
      (data,stat) = zookeeper.get(self.handle, node, None)
      zookeeper.delete(self.handle, node, stat["version"])
      return data
    except zookeeper.NoNodeException:
      # Someone deleted the node in between our get and delete
      return None
    except zookeeper.BadVersionException, e:
      # Someone is modifying the item in place. You can reasonably
      # either retry to re-read the item, or abort.
      print "Item %d modified in place, aborting..." % node
      raise e

# vim:sw=2:ts=2:et
