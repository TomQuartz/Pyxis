"""
Allocate a cluster of CloudLab machines to run Kayak/Splinter/Sandstorm.
Instructions:
"""

import geni.urn as urn
import geni.portal as portal
import geni.rspec.pg as rspec
import geni.aggregate.cloudlab as cloudlab
import json

# The possible set of base disk-images that this cluster can be booted with.
# The second field of every tupule is what is displayed on the cloudlab
# dashboard.
images = [("UBUNTU18-64-STD", "Ubuntu 18.04 (64-bit)")]

# The possible set of node-types this cluster can be configured with.
nodes = [("xl170", "xl170 (2 x E5-2640v4, 64 GB RAM, Mellanox ConnectX-4)")]

# Allows for general parameters like disk image to be passed in. Useful for
# setting up the cloudlab dashboard for this profile.
context = portal.Context()

# Default the disk image to 64-bit Ubuntu 16.04
context.defineParameter("image", "Disk Image",
                        portal.ParameterType.IMAGE, images[0], images,
                        "Specify the base disk image that all the nodes of the cluster " +
                        "should be booted with.")

# Default the node type to the d430.
context.defineParameter("type", "Node Type",
                        portal.ParameterType.NODETYPE, nodes[0], nodes,
                        "Specify the type of nodes the cluster should be configured with. " +
                        "For more details, refer to " +
                        "\"http://docs.cloudlab.us/hardware.html#%28part._apt-cluster%29\"")

context.defineParameter("roles", "Roles and # of Nodes",
                        portal.ParameterType.STRING, "{\"gateway\": 1, \"compute\": 4, \"storage\": 1}", [
                        ],
                        "Specify the roles and the number of nodes assuming each role. \
                        Should be a valid json str, example: {\"gateway\": 1, \"compute\": 4, \"storage\": 1}")

context.defineParameter("storage", "Extra Disk Space (GB)",
                        portal.ParameterType.INTEGER, 64)

context.defineParameter("mnt", "Mount Point for Extra Disk",
                        portal.ParameterType.STRING, "/workspace")

params = context.bindParameters()

names_json = json.loads(params.roles)
hostnames = []
for name, cnt in names_json.items():
    for i in range(cnt):
        hostnames.append(name + str(i+1))

request = rspec.Request()

# Create a local area network over a 10 Gbps.
lan = rspec.LAN()
lan.bandwidth = 10000000  # This is in kbps.

# Setup the cluster one node at a time.
for i in range(len(hostnames)):
    node = rspec.RawPC(hostnames[i])

    node.hardware_type = params.type
    node.disk_image = urn.Image(cloudlab.Utah, "emulab-ops:%s" % params.image)

    if len(params.mnt) > 0:
        bs = node.Blockstore("bs"+str(i), params.mnt)
        bs.size = str(params.storage) + 'GB'

    request.addResource(node)

    # Add this node to the LAN.
    iface = node.addInterface("eth0")
    lan.addInterface(iface)

# Add the lan to the request.
request.addResource(lan)

# Generate the RSpec
context.printRequestRSpec(request)
