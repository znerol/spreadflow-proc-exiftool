from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy
import unittest

from mock import Mock
from testtools.assertions import assert_that

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation
from twisted.internet import defer

from spreadflow_exiftool.proc import MetadataExtractor
from txexiftool import ExiftoolProtocol


class SpreadflowExiftoolTestCase(unittest.TestCase):
    def test_default_instance(self):
        """
        Verify default parameters.
        """
        sut = MetadataExtractor('exiftool')
        expected_args = (b'-j', b'-charset', b'exiftool=UTF-8', b'-charset', b'filename=UTF-8')

        self.assertEqual(sut.args, expected_args)
        self.assertEqual(sut.attrib, 'metadata')
        self.assertEqual(sut.buffersize, 2**22)
        self.assertEqual(sut.decode_errors, 'strict')
        self.assertEqual(sut.strport, 'exiftool')

    def test_single_job(self):
        """
        A single incoming message with a single file path.
        """
        sut = MetadataExtractor('exiftool', ('-some', '-arg'))

        sut.peer = Mock(spec=ExiftoolProtocol)
        return_value = defer.succeed(b'[{"hello": "world"}]')
        sut.peer.execute = Mock(return_value=return_value)

        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'path': '/path/to/file.jpg',
                }
            }
        }
        expected = copy.deepcopy(insert)
        expected['data']['a']['metadata'] = {'hello': 'world'}
        matches = MatchesSendDeltaItemInvocation(expected, sut)
        send = Mock(spec=Scheduler.send)
        sut(insert, send)
        self.assertEquals(send.call_count, 1)
        assert_that(send.call_args, matches)
        sut.peer.execute.assert_called_once_with(
            b'-some', b'-arg', b'-j', b'-charset', b'exiftool=UTF-8',
            b'-charset', b'filename=UTF-8', b'/path/to/file.jpg')
