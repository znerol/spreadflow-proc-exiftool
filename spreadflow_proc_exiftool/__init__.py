from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json
from twisted.internet import defer, protocol
from twisted.internet.endpoints import clientFromString
from twisted_exiftool import ExiftoolProtocol

class ExiftoolProtocolFactory(protocol.ClientFactory):

    def __init__(self, buffersize):
        self.buffersize = buffersize

    def buildProtocol(self, addr):
        proto = protocol.ClientFactory.buildProtocol(self, addr)
        proto.MAX_LENGTH = self.buffersize
        return proto


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
        jobs = [self.args + (item['data'][oid]['path'].encode('utf-8'),) for oid in item['inserts']]
        results = yield defer.DeferredList([self._protocol.execute(*job) for job in jobs])

        for oid, result in zip(item['inserts'], results):
            item['data'][oid][self.attrib] = json.loads(result[1], encoding='utf-8')[0]

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
