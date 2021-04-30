import collections, Pyro4

Pyro4.config.SERIALIZER = 'pickle'
ns = Pyro4.locateNS()

ProposalID = collections.namedtuple('ProposalID', ['number', 'uid'])

class PaxosMessage (object):
    # base class for all messages
    fromUID = None

class Prepare (PaxosMessage):
    def __init__(self, fromUID, proposalID):
        self.fromUID    = fromUID
        self.proposalID = proposalID

class Nack (PaxosMessage):
    def __init__(self, fromUID, proposerUID, proposalID, promisedProposalID):
        self.fromUID = fromUID
        self.proposalID = proposalID
        self.proposerUID = proposerUID
        self.promisedProposalID = promisedProposalID
        
class Promise (PaxosMessage):
    def __init__(self, fromUID, proposerUID, proposalID, lastAcceptedID, lastAcceptedValue):
        self.fromUID = fromUID
        self.proposerUID = proposerUID
        self.proposalID = proposalID
        self.lastAcceptedID = lastAcceptedID
        self.lastAcceptedValue  = lastAcceptedValue
        
class Accept (PaxosMessage):
    def __init__(self, fromUID, proposalID, proposalValue):
        self.fromUID = fromUID
        self.proposalID = proposalID
        self.proposalValue = proposalValue
    
class Accepted (PaxosMessage):
    def __init__(self, fromUID, proposalID, proposalValue):
        self.fromUID = fromUID
        self.proposalID = proposalID
        self.proposalValue = proposalValue
        
class InvalidMessageError (Exception):
   pass

class MessageHandler (object):
    @Pyro4.expose
    def receive(self, msg):
        # This function accepts any PaxosMessage subclass and then calls the relevant function
        handler = getattr(self, 'receive' + msg.__class__.__name__, None)
        if handler is None:
            raise InvalidMessageError('Receiving class does not support messages of type: ' + msg.__class__.__name__)
        return handler( msg )

@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class Proposer (MessageHandler):
    leader = False
    proposedValue = None
    proposalID = None
    highestAcceptedID = None
    promisesReceived = None
    acceptedRecieved = None
    nacksReceived = None
    currentPrepareMsg = None
    currentAcceptMsg = None
    
    def __init__(self, networkUID='A', quorumSize=1):
        self.networkUID = networkUID
        self.quorumSize = quorumSize
        self.proposalID = ProposalID(0, networkUID)
        self.highestProposalID = ProposalID(0, networkUID)
    
    def proposeValue(self, value):
        if self.proposedValue is None:
            self.proposedValue = value
            self.prepare()

    def prepare(self):
        self.leader = False
        self.promisesReceived = set()
        self.nacksReceived = set()
        self.acceptedRecieved = set()
        self.proposalID = ProposalID(self.highestProposalID.number + 1, self.networkUID)
        self.highestProposalID = self.proposalID
        self.currentPrepareMsg = Prepare(self.networkUID, self.proposalID)
        print('Prepare: ', self.networkUID, self.proposalID)
        # broadcast to acceptor(s)
        uri = ns.lookup('C')
        node = Pyro4.Proxy(uri)
        node.receive(self.currentPrepareMsg)
            
    def receiveNack(self, msg):
        if msg.proposalID == self.proposalID and self.nacksReceived is not None:
            self.nacksReceived.add( msg.fromUID )
            # if true, this Proposer failed to achieve leadership
            if len(self.nacksReceived) == self.quorumSize:
                return self.prepare()

    def receivePromise(self, msg):
        # Returns an Accept messages if a quorum of Promise messages 
        if not self.leader and msg.proposalID == self.proposalID and msg.fromUID not in self.promisesReceived:
            self.promisesReceived.add(msg.fromUID)
            if msg.lastAcceptedID > self.highestAcceptedID:
                self.highestAcceptedID = msg.lastAcceptedID
                if msg.lastAcceptedValue is not None:
                    self.proposedValue = msg.lastAcceptedValue
            # Proposer will become leader when it has recieved as many promises as the quorumSize
            if len(self.promisesReceived) == self.quorumSize:
                self.leader = True
            # return Accept message iff a proposed value has been set
            if self.proposedValue is not None:
                self.currentAcceptMsg = Accept(self.networkUID, self.proposalID, self.proposedValue)
                # reply to node
                uri = ns.lookup(msg.fromUID)
                node = Pyro4.Proxy(uri)
                node.receive(self.currentAcceptMsg)

    def receiveAccepted(self, msg):
        self.acceptedRecieved.add(msg.fromUID)
        if len(self.acceptedRecieved) == self.quorumSize:
            self.proposeValue = None
    
    def updateQuorumSize(self, quorumSize):
        self.quorumSize = quorumSize
        
@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class Acceptor (MessageHandler):
    def __init__(self, networkUID='B', promised_id=None, accepted_id=None, accepted_value=None):
        self.networkUID = networkUID
        self.promised_id = promised_id
        self.accepted_id = accepted_id
        self.accepted_value = accepted_value
        
    def receivePrepare(self, msg):
        uri = ns.lookup(msg.fromUID)
        node = Pyro4.Proxy(uri)
        # return Promise iff the proposalID is >= any promised id
        if msg.proposalID >= self.promised_id:
            self.promised_id = msg.proposalID
            node.receive(Promise(self.networkUID, msg.fromUID, self.promised_id, self.accepted_id, self.accepted_value))
            print('Promise: ', self.networkUID, msg.fromUID, self.promised_id, self.accepted_id, self.accepted_value)
        else:
            node.receive(Nack(self.networkUID, msg.fromUID, msg.proposalID, self.promised_id))
       
    def receiveAccept(self, msg):
        uri = ns.lookup(msg.fromUID)
        node = Pyro4.Proxy(uri)
        # return Accepted iff the proposalID is >= any promised id
        if msg.proposalID >= self.promised_id:
            self.promised_id = msg.proposalID
            self.accepted_id = msg.proposalID
            self.accepted_value  = msg.proposalValue
            accepetedMsg = Accepted(self.networkUID, msg.proposalID, msg.proposalValue)
            print('Accepted: ', self.networkUID, msg.proposalID, msg.proposalValue)
            # reply to proposer
            node.receive(accepetedMsg)
            # also broadcast to learner(s)
            uri = ns.lookup('C')
            learner = Pyro4.Proxy(uri)
            learner.receive(accepetedMsg)
        else:
            node.receive(Nack(self.networkUID, msg.fromUID, msg.proposalID, self.promised_id))

@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class Learner (MessageHandler):
    class ProposalStatus (object):
        __slots__ = ['acceptCount', 'retainCount', 'acceptors', 'value']
        def __init__(self, value):
            self.acceptCount = 0
            self.retainCount = 0
            self.acceptors = set()
            self.value = value

    def __init__(self, networkUID='C', quorumSize=1):
        self.networkUID = networkUID
        self.quorumSize = quorumSize
        self.proposals = dict()
        self.acceptors = dict()
        self.finalValue = None
        self.finalAcceptors = None
        self.finalProposalID = None
        
    def receiveAccepted(self, msg):
        # saves the proposal value into the state 
        if msg.proposalID >= self.finalProposalID and msg.proposalValue == self.finalValue:
            self.finalAcceptors.add(msg.fromUID)
            print('Final value: ' + self.finalValue)
            return
        
        last_pn = self.acceptors.get(msg.fromUID)

        # old message
        if msg.proposalID <= last_pn:
            return

        self.acceptors[msg.fromUID] = msg.proposalID
        
        if last_pn is not None:
            ps = self.proposals[last_pn]
            ps.retainCount -= 1
            ps.acceptors.remove(msg.fromUID)
            if ps.retainCount == 0:
                del self.proposals[last_pn]

        if not msg.proposalID in self.proposals:
            self.proposals[msg.proposalID] = Learner.ProposalStatus(msg.proposalValue)

        ps = self.proposals[msg.proposalID]
        ps.acceptCount += 1
        ps.retainCount += 1
        ps.acceptors.add(msg.fromUID)

        if ps.acceptCount == self.quorumSize:
            self.finalProposalID = msg.proposalID
            self.finalValue = msg.proposalValue
            self.finalAcceptors = ps.acceptors
            self.proposals = None
            self.acceptors = None
            print('Final value: ' + self.finalValue)
            return
    
    def updateQuorumSize(self, quorumSize):
        self.quorumSize = quorumSize
