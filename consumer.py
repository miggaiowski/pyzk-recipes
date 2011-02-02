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
ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"};

MIN_ARGS = 1
__VERSION__ = 0.1

def consumer(host, port, id, delay = 0.1):
  """
  Connects to zookeeper and consumes all the items on the queue
  'myfirstqueue' on every @delay seconds
  """
  zk = queue.ZooKeeperQueue("myfirstqueue", host, port)

  try:
    while True:
      v = zk.dequeue()
      while v != None:
        print "Consumer %s: %s" % (id, v)
        sys.stdout.flush()
        time.sleep(delay)
        v = zk.dequeue()
      print "Nothing to be consumed, sleeping 2 seconds"
      time.sleep(2)
  except KeyboardInterrupt:
    pass
  zk.__del__()
  print "Done"

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
        'i' : ['id',
            "Change the ID of the consumer (identity when printing)",
            ""],
    }

    options_list = ' '.join(["[-%s --%s]" % (o, options[o][0]) for o in options])
    desc = consumer.__doc__.replace('  ','')
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

    consumer(opt.H, int(args[1]), opt.i)

# vim:sw=2:ts=2:et
