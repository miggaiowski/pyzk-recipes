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

# Grupo 03

import zookeeper, threading, sys, time, queue

NUMPRODUCTS = 10

if __name__ == '__main__':
  if len(sys.argv) < 3:
    print "Usage: python ", sys.argv[0], " PORTNUMBER", "ID"
    sys.exit(1)
  zk = queue.ZooKeeperQueue("myfirstqueue", int(sys.argv[1]), is_producer=True)
  ID = sys.argv[2]
  try:
    lastproduct = 0
    while True:
      # Enqueueing new items, until we have a buffer of NUMPRODUCTS
      next_item = zk.get_and_maintain() # "LASTPRODUCT ID"
      while not next_item or lastproduct - int(next_item.split()[0]) < NUMPRODUCTS:
        lastproduct += 1
        zk.enqueue("%d %s" % (lastproduct, ID))
        print "Enqueued %d %s" % (lastproduct, ID)
        next_item = zk.get_and_maintain()
        time.sleep(0.04)
  except KeyboardInterrupt:
    pass
  zk.__del__()
  print "Done"
  
