#!/usr/bin/env python3.11

from network import NetworkTopology, DirectDriveNetwork


print("Initializing Topology")
topology = NetworkTopology()
print("Initializing Network")
network = DirectDriveNetwork(
    topology=topology, slice_size=512, disk_size=4096)

print("Adding Read Interaction")
network.add_read(0, 0, 20)
print("Adding Write Interaction")
network.add_write(0, 0, 20)
print("Injecting Interactions")
network.inject_interactions()
print("Creating Goal Content")
goal_res = network.to_goal()
print("Goal Content:")
print(goal_res)
print("Writing Goal Content to 'out.goal'")
with open('out.goal', 'w+') as f:
    print(goal_res, file=f)
