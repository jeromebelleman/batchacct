#! /usr/bin/env python

import os
import unittest
import pdb
import tempfile
import common

L = '"timestamp=1970-10-10 10:42:00" "userDN=theUserDN" "userFQAN=theUserFQAN" "ceID=theCEID" "jobID=theJobID" "lrmsID=42" "localUser=42"'

class EventHandler():
    pass

class TestWhisk(unittest.TestCase):
    def test_parse(self):
        # Create simple-minded event handler
        evthdl = EventHandler()
        evthdl.buf = ''
        evthdl.offset = 0

        # Create temporary accounting file
        fd, path = tempfile.mkstemp()
        w = os.fdopen(fd, 'w')
        r = open(path, 'r')

        # Write the first bit to it
        l = L[:40]
        print "writing " + l
        w.write(l)
        w.flush()
        os.fsync(fd)
        self.assertEqual(list(common.parse(r, evthdl)), [])

        # Write the remainder to it
        l = L[40:]
        print "writing " + l
        w.write(l + '\n')
        w.flush()
        os.fsync(fd)
        self.assertEqual(len(list(common.parse(r, evthdl))), 1)

        # Cleanup
        r.close()
        w.close()
