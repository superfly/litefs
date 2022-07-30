LiteFS
![GitHub release (latest by date)](https://img.shields.io/github/v/release/superfly/litefs)
![Status](https://img.shields.io/badge/status-alpha-yellow)
![GitHub](https://img.shields.io/github/license/superfly/litefs)
======

LiteFS is a FUSE-based file system for replicating SQLite databases across a
cluster of machines. It works as a passthrough file system that intercepts
writes to SQLite databases in order to detect transaction boundaries and record
changes on a per-transaction level in [LTX files](https://github.com/superfly/ltx).

This project is actively maintained but is currently in an alpha state. The file
system and replication are functional but testing & hardening are needed to make
it production-ready. This repository is open source in order to collect feedback
and ideas for how to make SQLite replication better.


## Example usage

The following tutorial demonstrates how to use LiteFS to replicate a SQLite
database across two live systems: a primary system and a secondary system.

The primary system can write to its local SQLite database, and LiteFS will
replicate the changes to the secondary system. The secondary system will be a
read-only replica of the primary. It won't be able to make changes, but if the
primary system becomes unavailable, the secondary system will take over as the
primary node.

### Install dependencies

First, install the required packages on each system where you'll be running
LiteFS.

```sh
apt install fuse3 libfuse-dev sqlite3 consul wget tar
```

### Install LiteFS

Next, download and install the LiteFS binary on each system:

```sh
ARCH="amd64" # Change to your system's architecture.
VERSION="0.1.0" # Change to the latest LiteFS version
pushd $(mktemp --directory) && \
  wget "https://github.com/superfly/litefs/releases/download/v${VERSION}/litefs-v${VERSION}-linux-${ARCH}.tar.gz" && \
  tar xvf "litefs-v${VERSION}-linux-${ARCH}.tar.gz" && \
  sudo mv litefs /usr/local/bin && \
popd
```

### Start Consul on primary node

LiteFS uses Consul for leader election.

On your primary node, run Consul in development mode:

```sh
consul agent -client "::" -dev
```

### Configure your primary node

First, choose the location for your SQLite database on your primary node:

```sh
# Choose where you want to store your SQLite database.
export DB_FILE="${HOME}/litefs-demo1/data.db"

# Make sure parent directory exists.
export DB_DIR="$(dirname ${DB_FILE})"
mkdir --parents "${DB_DIR}"
```

Then, choose the hostname for your primary node.

```sh
# Leave as-is if both nodes are on the same network. Otherwise, change to a
# domain name or IP that other nodes can use to connect with this node.
export HOSTNAME="$(hostname --fqdn)"
```

Then, choose the port on which LiteFS will listen for connections from other
LiteFS nodes.

```sh
export LITEFS_PORT="20202"
```

Then, specify the URL of the Consul server. If it's running on the same node
as your primary node, you can leave it as the default value:

```sh
export CONSUL_URL="http://localhost:8500"
```

Finally, create your LiteFS config file. Create a file called `litefs.yml` and
populate it with the following.

```yml
mount-dir: "${DB_DIR}"

http:
  addr: ":${LITEFS_PORT}"

consul:
  url: "${CONSUL_URL}"
  advertise-url: "http://${HOSTNAME}:${LITEFS_PORT}"
```

LiteFS expands environment variables, so you can leave the environment variable
names in the file, and LiteFS will expand them at runtime.

For more details on LiteFS's configuration options, see the
[example config](cmd/litefs/etc/litefs.yml).


### Launch primary LiteFS node

Now that your configuration is done, it's time to launch LiteFS:

```sh
litefs -config litefs.yml
```

If everything worked, you should see a message like this:

```text
primary lease acquired, advertising as http://bert.localdomain:20202
LiteFS mounted to: /home/user/litefs-demo1
http server listening on: http://bert:20202
stream connected
```


### Configure your secondary node

Now that we have your primary node up and running, it's time to create a
secondary LiteFS node.

First, choose the directory that LiteFS will use to replicate SQLite databases:

```sh
export DB_DIR="${HOME}/litefs-demo2"
mkdir --parents "${DB_DIR}"
```

Then, choose the hostname through which other nodes can connect to this node.

```sh
# Leave as-is if both nodes are on the same network. Otherwise, change to a
# domain name or IP that other nodes can use to connect with this node.
export HOSTNAME="$(hostname --fqdn)"
```

Then, choose the port on which LiteFS will listen for connections from other
LiteFS nodes.

```sh
export LITEFS_PORT="30303"
```

Then, specify the URL of the Consul server on the primary node.:

```sh
# Change to the domain name or IP of your primary node.
export PRIMARY_NODE="bert"
export CONSUL_URL="http://${PRIMARY_NODE}:8500"
```

Finally, create your LiteFS config file. Create a file called `litefs.yml` and
populate it with the following.

```yml
mount-dir: "${DB_DIR}"

http:
  addr: ":${LITEFS_PORT}"

consul:
  url: "${CONSUL_URL}"
  advertise-url: "http://${HOSTNAME}:${LITEFS_PORT}"
```

### Launch secondary LiteFS node

Just as you did with your first node, it's time to run LiteFS on your secondary
node:

```sh
litefs -config litefs.yml
```

If everything worked, you should see a message like this:

```text
initializing consul: key=http://bert:8500 url=litefs/primary advertise-url=http://ernie.localdomain:
LiteFS mounted to: /home/user/example-data2
http server listening on: http://localhost:30303
existing primary found (http://bert.localdomain:20202), connecting as replica
```


### Replicating data across nodes

Go back to your primary node, and start a new terminal session. Create a new
SQLite database and populate it with some data.

```sh
# Use the same database file you specified above.
DB_FILE="${HOME}/litefs-demo1/data.db"

sqlite3 "${DB_FILE}" 'CREATE TABLE movies (title TEXT, rating INT)'
sqlite3 "${DB_FILE}" 'INSERT INTO movies (title, rating) VALUES ("The Jerk", 10)'
sqlite3 "${DB_FILE}" 'INSERT INTO movies VALUES ("Election", 9)'
```

Now, switch to your secondary node and see if LiteFS replicated the data:

```sh
# Use the same database file you specified above.
DB_FILE="${HOME}/litefs-demo2/data.db"

sqlite3 "${DB_FILE}" 'SELECT * FROM movies'
```

You should see the data that you added from your primary node:

```text
The Jerk|10
Election|9
```

### Failing over to your secondary node

From your secondary node, try adding some data to the database:

```sh
$ sqlite3 "${DB_FILE}" 'INSERT INTO movies VALUES ("Chairman of the Board", 1)'
Error: unable to open database file
```

Whoops. Your secondary node can't write to the database.

This is by design. To ensure the integrity of the data, only one node can act
as a writer. All the other nodes are read-only, and they will see an error if
they attempt to write to the database.

If the primary node becomes unavailable, Consul will appoint a new primary. To
see that in action, return to the LiteFS session on your primary node, and use
Ctrl+C to kill the process.

You should see this messages:

```text
signal received, litefs shutting down
stream disconnected
exiting primary, destroying lease
```

Now, go back to your secondary node and try the `INSERT` query again:

```sh
sqlite3 "${DB_FILE}" 'INSERT INTO movies VALUES ("Chairman of the Board", 1)'
```

This time, the `INSERT` worked because your secondary node has taken over as
your primary.

You can see the new row in the database:

```sh
sqlite3 "${DB_FILE}" "SELECT * FROM movies"
The Jerk|10
Election|9
Chairman of the Board|1
```


### Caveats

If `litefs` does not exit cleanly then you may need to manually run `umount` to
unmount the file system before re-mounting it:

```sh
umount -f /path/to/mnt
```

`litefs` will not unmount cleanly if there is a SQLite connection open so be
sure to close your application or `sqlite3` sessions before unmounting.



## Architecture

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

Currently, LiteFS only supports the SQLite rollback journal but it will support
WAL mode and possibly [`wal2`](https://www.sqlite.org/cgi/src/doc/wal2/doc/wal2.md)
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



## Contribution Policy

LiteFS is open to code contributions for bug fixes only. Features carry
a long-term maintenance burden so they will not be accepted at this time.
Please [submit an issue][new-issue] if you have a feature you'd like to
request.

[new-issue]: https://github.com/superfly/litefs/issues/new
