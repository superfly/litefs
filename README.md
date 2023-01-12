LiteFS
![GitHub release (latest by date)](https://img.shields.io/github/v/release/superfly/litefs)
![Status](https://img.shields.io/badge/status-beta-blue)
![GitHub](https://img.shields.io/github/license/superfly/litefs)
======

LiteFS is a FUSE-based file system for replicating SQLite databases across a
cluster of machines. It works as a passthrough file system that intercepts
writes to SQLite databases in order to detect transaction boundaries and record
changes on a per-transaction level in [LTX files](https://github.com/superfly/ltx).

This project is actively maintained but is currently in a beta state. Please
report any bugs as an issue on the GitHub repository.

You can find a [Getting Started guide](https://fly.io/docs/litefs/getting-started/)
on [LiteFS' documentation site](https://fly.io/docs/litefs/). Please see the
[ARCHITECTURE.md](/docs/ARCHITECTURE.md) design document for details about how
LiteFS works.


## SQLite TCL Test Suite

It's a goal of LiteFS to pass the SQLite TCL test suite, however, this is
currently a work in progress. LiteFS doesn't have database deletion implemented
yet so that causes many tests to fail during teardown.

To run a test from the suite against LiteFS, you can use the `Dockerfile.test`
to run it in isolation. First build the Dockerfile:

```sh
docker build -t litefs-test -f Dockerfile.test .
```

Then run it with the filename of the test you want to run. In this case, we
are running `select1.test`:

```sh
docker run --device /dev/fuse --cap-add SYS_ADMIN -it litefs-test select1.test
```


## Contributing

LiteFS contributions work a little different than most GitHub projects. If you
have a small bug fix or typo fix, please PR directly to this repository.

If you would like to contribute a feature, please follow these steps:

1. Discuss the feature in an issue on this GitHub repository.
2. Create a pull request to **your fork** of the repository.
3. Post a link to your pull request in the issue for consideration.

This project has a roadmap and features are added and tested in a certain order.
Additionally, it's likely that code style, implementation details, and test
coverage will need to be tweaked so it's easier to for me to grab your
implementation as a starting point when implementing a feature.
