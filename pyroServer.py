import Pyro4

from paxos import Proposer, Acceptor, Learner

Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')

ns = Pyro4.locateNS()

daemon = Pyro4.Daemon()

proposerURI = daemon.register(Proposer)
ns.register('A', proposerURI)
print('Proposer1: ' + str(proposerURI))

acceptorURI = daemon.register(Acceptor)
ns.register('B', acceptorURI)
print('Acceptor: ' + str(acceptorURI))

learnerURI = daemon.register(Learner)
ns.register('C', learnerURI)
print('Learner: ' + str(learnerURI))

daemon.requestLoop()
