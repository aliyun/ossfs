#!/usr/bin/env python

import os
import random
import sys

# Cover issue 320
# https://github.com/s3fs-fuse/s3fs-fuse/issues/320

mountpoint = sys.argv[1]

filename = os.path.join(mountpoint, "ossfs_test_%d" % random.randint(0, 1000000))
data = "hello"
f = open(filename, 'w')
f.write(data)
f.flush()
sz = os.stat(filename).st_size
os.remove(filename)
assert sz == len(data), "%d, %d"%(sz, len(data))

# suppose the "multipart_size" is set to 1 MB.
data = '0' * (2 * 1024 * 1024 + 12)
f = open(filename, 'w')
f.write(data)
f.flush()
sz = os.stat(filename).st_size
os.remove(filename)
assert sz == len(data), "%d, %d"%(sz, len(data))
