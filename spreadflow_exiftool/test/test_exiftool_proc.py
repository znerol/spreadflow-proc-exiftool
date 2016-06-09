from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy
import unittest

from mock import Mock
from testtools import matchers, twistedsupport
from testtools.assertions import assert_that

from spreadflow_core.scheduler import Scheduler
from spreadflow_delta.test.matchers import MatchesSendDeltaItemInvocation
from twisted.internet import defer

from spreadflow_exiftool.proc import MetadataExtractor, MetadataExtractorError
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
        sut.peer.execute.return_value = defer.succeed(b'[{"hello": "world"}]')

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

    def test_single_fail(self):
        """
        A single incoming message with a single file path.
        """
        sut = MetadataExtractor('exiftool', ('-some', '-arg'))

        error = RuntimeError('boom!')

        sut.peer = Mock(spec=ExiftoolProtocol)
        sut.peer.execute.return_value = defer.fail(error)

        insert = {
            'inserts': ['a'],
            'deletes': [],
            'data': {
                'a': {
                    'path': '/path/to/file.jpg',
                }
            }
        }
        send = Mock(spec=Scheduler.send)
        result = sut(insert, send)
        self.assertEquals(send.call_count, 0)
        sut.peer.execute.assert_called_once_with(
            b'-some', b'-arg', b'-j', b'-charset', b'exiftool=UTF-8',
            b'-charset', b'filename=UTF-8', b'/path/to/file.jpg')

        failure_matcher = matchers.AfterPreprocessing(
            lambda f: f.value, matchers.IsInstance(MetadataExtractorError))
        assert_that(result, twistedsupport.failed(failure_matcher))
