LiteFS Architecture
===================

The LiteFS system is composed of 3 major parts:

1. FUSE file system: intercepts file system calls to record transactions.
1. Leader election: currently implemented by [Consul](https://www.consul.io/) using sessions
1. HTTP server: provides an API for replica nodes to receive changes.


### Lite Transaction Files (LTX)

Each transaction in SQLite is simply a collection of one or more pages to be
written. This is done safely by the rollback journal or the write-ahead log (WAL)
within a SQLite database.

An [LTX file](https://github.com/superfly/ltx) is an additional packaging format
for these change sets. Unlike the journal or the WAL, the LTX file is optimized
for use in a replication system by utilizing the following:

- Checksumming across the LTX file to ensure consistency.
- Rolling checksum of the entire database on every transaction.
- Sorted pages for efficient compactions to ensure fast recovery time.
- Page-level encryption (future work)
- Transactional event data (future work)

Each LTX file is associated with an autoincrementing transaction ID (TXID) so
that replicas can know their position relative to the primary node. This TXID
is also associated with a rolling checksum of the entire database to ensure that
the database is never corrupted if a split brain occurs. Please see the
_Guarantees_ section to understand how async replication and split brain works.


### File system

The FUSE-based file system allows the user to mount LiteFS to a directory. For
the primary node in the cluster, this means it can intercept write transactions
via the file system interface and it is transparent to the application and SQLite.

For replica nodes, the file system adds protections by ensuring databases are
not writeable. The file system also provides information about the current
primary node to the application via the `.primary` file.

In SQLite, write transactions work by copying pages out to the rollback journal,
updating pages in the database file, and then deleting the rollback journal when
complete. LiteFS passes all these file system calls through to the underlying
files, however, it intercepts the journal deletion at the end to convert the
updated pages to an LTX file.

Currently, LiteFS supports both the rollback and WAL mode journal mechanisms.
It will possibly support [`wal2`](https://www.sqlite.org/cgi/src/doc/wal2/doc/wal2.md)
in the future.


### Leader election

Because LiteFS is meant to be used in ephemeral deployments such as
[Fly.io](https://fly.io/) or [Kubernetes](https://kubernetes.io/), it cannot use
a distributed consensus algorithm that requires strong membership such as Raft.
Instead, it delegates leader election to Consul sessions and uses a time-based
lease system.

Distributed leases work by obtaining a lock on a key within Consul which
guarantees that only one node can be the primary at any given time. This lease
has a time-to-live (TTL) which is automatically renewed by the primary as long
as it is alive. If the primary shuts down cleanly, the lease is destroyed and
another node can immediately become the new primary. If the primary dies
unexpectedly then the TTL must expire before a new node will become primary.

Since LiteFS uses async replication, replica nodes may be at different
replication positions, however, whichever node becomes primary will dictate the
state of the database. This means replicas which are further ahead could
potentially lose some transactions. See the _Guarantees_ section below for more
information.


### HTTP server

Replica nodes communicate with the primary node over HTTP. When they connect to
the primary node, they specify their replication position, which is their
transaction ID and a rolling checksum of the entire database. The primary node
will then begin sending transaction data to the replica starting from that
position. If the primary no longer has that transaction position available, it
will resend a snapshot of the current database and begin replicating
transactions from there.


## Guarantees

LiteFS is intended to provide easy, live, asychronous replication across
ephemeral nodes in a cluster. This approach makes trade-offs as compared with
simpler disaster recovery tools such as [Litestream](https://litestream.io/) and
more complex but strongly-consistent tools such as
[rqlite](https://github.com/rqlite/rqlite).

As with any async replication system, there's a window of time where
transactions are only durable on the primary node and have not been replicated
to a replica node. A catastrophic crash on the primary would cause these
transactions to be lost. Typically, this window is subsecond as transactions can
quickly be shuttled from the primary to the replicas.

Synchronous replication and time-bounded asynchronous replication is planned for
future versions of LiteFS.


### Ensuring consistency during split brain

Because LiteFS uses async replication, there is the potential that a primary
could receive writes but is unable to replicate them during a network partition.
If the primary node loses its leader status and later connects to the new leader,
its database state will have diverged from the new leader. If it naively began
applying transactions from the new leader, it could corrupt its database state.

Instead, LiteFS utilizes a rolling checksum which represents a checksum of the
entire database at every transaction. When the old primary node connects to the
new primary node, it will see that its checksum is different even though its
transaction ID could be the same. At this point, it will resnapshot the database
from the new primary to ensure consistency.


### Rolling checksum implementation

The rolling checksum is implemented by combining checksums of every page
together. When a page is written, LiteFS will compute the CRC64 of the page
number and the page data and XOR them into the rolling checksum. It will also
compute this same page checksum for the old page data and XOR that value out
of the rolling checksum.

This approach gives us strong guarantees about the exact byte contents of the
database at every transaction and it is fast to compute. As XOR is associative,
it is also possible to compute on a raw database file from scratch to ensure
consistency.

