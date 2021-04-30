import sys, Pyro4

if sys.version_info < (3, 0):
  input = raw_input

Pyro4.config.SERIALIZER = 'pickle'

ns = Pyro4.locateNS()

uri = ns.lookup('A')
proposer = Pyro4.Proxy(uri)

proposeValue1 = input("Enter the propose value: ").strip()
proposer.proposeValue(proposeValue1)

'''
# prepare msg
prepareMsg = proposer.prepare()

# promise msg
promiseMsg = acceptor.receive(prepareMsg)

# accept msg
acceptMsg = proposer.receive(promiseMsg)

# accepted msg
acceptedMsg = acceptor.receive(acceptMsg)

#send accepted to proposer
proposer.receive(acceptedMsg)

# forward accepted to learner
learner.receive(acceptedMsg)

print(finalValue)
'''