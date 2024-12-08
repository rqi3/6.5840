Implementation of the extended [Raft](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf) paper, done as part of 6.5840 (Distributed Systems, Graduate-level) at MIT during Spring 2024.

## Lab 1: MapReduce
A worker process that calls application Map and Reduce functions and handles reading and writing files, and a coordinator process that hands out tasks to workers and copes with failed workers. 
## Lab 2: Key/Value Server
A key/value server for a single machine that ensures that each operation is executed exactly once despite network failures and that the operations are linearizable.
## Lab 3A: leader election
Raft leader election and heartbeats (AppendEntries RPCs with no log entries). The goal for Part 3A is for a single leader to be elected, for the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost. 
## Lab 3B: log
Leader and follower code to append new log entries.
## Lab 3C: persistence
If a Raft-based server reboots it should resume service where it left off. This requires that Raft keep persistent state that survives a reboot. The paper's Figure 2 mentions which state should be persistent.
## Lab 4: Fault-tolerant Key/Value Service
Your key/value service will be a replicated state machine, consisting of several key/value servers that each maintain a database of key/value pairs.
## Lab 5: Sharded Key/Value Service
In this lab you'll build a key/value storage system that "shards," or partitions, the keys over a set of replica groups. A shard is a subset of the key/value pairs; for example, all the keys starting with "a" might be one shard, all the keys starting with "b" another, etc. The reason for sharding is performance. Each replica group handles puts and gets for just a few of the shards, and the groups operate in parallel; thus total system throughput (puts and gets per unit time) increases in proportion to the number of groups.

Implemented a no-credit challenge exercise: Client requests during configuration changes.
