# Xenon glossary


- **History shard:** Temporal's logical grouping of workflow executions, owned by a History service instance.
- **Storage partition:** Xenon's unit of atomic storage and write ownership; exact layout remains undecided.
- **Xenon node:** A process serving persistence operations for the storage partitions it currently owns.
- **Persistence adapter:** Code inside Temporal implementing persistence interfaces and communicating with Xenon.
- **Ownership handover:** Transfer of the right to commit writes for a storage partition from one node to another.
- **Acknowledged write:** A persistence operation for which Xenon returned success; its committed effects must survive loss of the serving process and local disk.
- **Scale-out:** Adding serving nodes and distributing existing work across them.
- **Partition splitting:** Dividing one logical storage partition into multiple partitions; distinct from moving an existing partition.
