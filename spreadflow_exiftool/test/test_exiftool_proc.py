from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from twisted.trial import unittest

from spreadflow_exiftool.proc import MetadataExtractor


class SpreadflowExiftoolTestCase(unittest.TestCase):
    def test_default_instance(self):
        proc = MetadataExtractor('exiftool')
        expected_args = (b'-j', b'-charset', b'exiftool=UTF-8', b'-charset', b'filename=UTF-8')

        self.assertEqual(proc.args, expected_args)
        self.assertEqual(proc.attrib, 'metadata')
        self.assertEqual(proc.buffersize, 2**22)
        self.assertEqual(proc.decode_errors, 'strict')
        self.assertEqual(proc.strport, 'exiftool')
