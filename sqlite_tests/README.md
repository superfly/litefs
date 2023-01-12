Sqlite tests
======

(Very rough) Tools to build and run Sqlite TCL tests again LiteFS.

How to build and run tests?

1. From root directory of LiteFS source tree run `docker build -f sqlite_test/Dockerfile .`
2. Execute the tests: `docker run --device /dev/fuse --cap-add SYS_ADMIN -it <image generated in step 1> <testsuite name>`.
    1. Ex: `docker run --device /dev/fuse --cap-add SYS_ADMIN -it 7b17b406449e select1` will run the `select1.tcl` test

Notes:
1. Debug FUSE logging is currently enabled in litefs.yml to better trace errors running test suites

TODOs:
1. Parameritize Sqlite version 
2. Cleanup unzipping Sqlite source
3. Optimize Docker image (removing Sqlite source, only copying needed binaries)
4. Remove unneeded apt packages (nano, procps, kmod)
5. Switch off debug logging in litefs.yml
