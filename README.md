# basicpaxos

Please use Python version 2.x.x. as it will not work with Python3.

The code uploaded can be used to simulate a Basic Paxos run with one Proposer, one Acceptor, and one Learner, locally.

Steps to run:
## 1. Install Pyro4
``` bash
# install Pryo4
pip2 install Pyro4
```
## 2. Start Pyro4 Name Server
``` bash
# tell name server to use pickle
export PYRO_SERIALIZERS_ACCEPTED=serpent,pickle
# start Name Server
python -m Pyro4.naming
```
## 3. Start Pyro4 Daemon
``` bash
# run pyroServer.py
python pyroServer.py
```

## 4. Run Client File
``` bash
# run client.py
python pyroServer.py
```

## 5. Done!

If you would like to create more nodes for testing, you must update a few functions. Each node has thier networkUID and the quorum size defined in the __init__ functions as follows:
``` bash
def __init__(self, networkUID='A', quorumSize=1):
```
These funcations must be updated to reflect the correct networkUID you would like to assign, and the updated quorum size.

## Using Pyro Over a Network
To host the Paxos roles on different machines, each node must have it's own Pyro Server. The uploaded pyroServer.py file can be modified to host a prefered role.
When doing Pyro calls over a network, you must define the host and port for each server, as well as the name server, or they will just be hosted on localhost.
This can be done as follows:
``` bash
# start name server allowing it to be accessable to other machines
python -m Pyro4.naming -n IP_ADDR
# locate remote name server from code
ns = Pyro4.locateNS(host='IP_ADDR', port=9090)
# create pyro daemon to be accessable to other machines
daemon = Pyro4.Daemon(host='IP_ADDR', port=9090)
```

I always used port 9090, but you can assign any available port.

## Thank you!
