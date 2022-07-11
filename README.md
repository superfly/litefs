litefs
======

A FUSE filesystem for replicating SQLite databases.


## Development

To develop & test LiteFS, you must run an instance of Consul. To run a Consul
agent in dev mode, run the following:

```sh
consul agent -dev
```

Consul should then be available at http://localhost:8500

