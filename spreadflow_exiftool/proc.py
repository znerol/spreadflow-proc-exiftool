from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json
from twisted.internet import defer, protocol
from twisted.internet.endpoints import clientFromString
from txexiftool import ExiftoolProtocol

class ExiftoolProtocolFactory(protocol.ClientFactory):

    def __init__(self, buffersize):
        self.buffersize = buffersize

    def buildProtocol(self, addr):
        proto = protocol.ClientFactory.buildProtocol(self, addr)
        proto.MAX_LENGTH = self.buffersize
        return proto


class MetadataExtractorError(Exception):
    def __init__(self, path, cause):
        super(MetadataExtractorError, self).__init__()
        self.path = path
        self.cause = cause

    def __str__(self):
        return "Failed to extract metadata from {:s}: {:s}".format(
            self.path, self.cause)

class MetadataExtractor(object):

    reactor = None

    def __init__(self, endpoint, args = (), attrib='metadata', buffersize=2**22):
        self.factory = ExiftoolProtocolFactory.forProtocol(ExiftoolProtocol, buffersize)
        self.endpoint = endpoint
        self._protocol = None

        self.args = tuple(arg if isinstance(arg, bytes) else arg.encode('utf-8') for arg in args) + (b'-j', b'-charset', b'exiftool=UTF-8', b'-charset', b'filename=UTF-8')
        self.attrib = attrib

    @defer.inlineCallbacks
    def __call__(self, item, send):
        def _job_callback(result, oid):
            return oid, self.attrib, json.loads(result, encoding='utf-8')[0]

        failures = []
        def _job_errback(failure, oid):
            failures.append((oid, failure))

        jobs = []
        for oid in item['inserts']:
            args = self.args + (item['data'][oid]['path'].encode('utf-8'),)
            job = self._protocol.execute(*args)
            job.addCallback(_job_callback, oid)
            job.addErrback(_job_errback, oid)
            jobs.append(job)

        results = yield defer.DeferredList(jobs)

        if len(failures):
            oid, failure = failures[0]
            raise MetadataExtractorError(item['data'][oid]['path'], failure)

        for status, (oid, attrib, data) in results:
            item['data'][oid][attrib] = data

        send(item, self)

    def attach(self, dispatcher, reactor):
        self.reactor = reactor

    @defer.inlineCallbacks
    def start(self):
        self._protocol = yield clientFromString(self.reactor, 'exiftool').connect(self.factory)

    @defer.inlineCallbacks
    def join(self):
        if self._protocol:
            yield self._protocol.loseConnection()
            self._protocol = None

    def detach(self):
        self.reactor = None
