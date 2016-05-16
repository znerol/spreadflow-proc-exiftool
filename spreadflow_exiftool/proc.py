from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json
from twisted.internet import defer, protocol
from txexiftool import ExiftoolProtocol

from spreadflow_core.remote import ClientEndpointMixin

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

class MetadataExtractor(ClientEndpointMixin):

    def __init__(self, strport, args=(), attrib='metadata', buffersize=2**22):
        self.buffersize = buffersize
        self.strport = strport

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
            job = self.peer.execute(*args)
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

    def get_client_protocol_factory(self, scheduler, reactor):
        return ExiftoolProtocolFactory.forProtocol(ExiftoolProtocol, self.buffersize)
