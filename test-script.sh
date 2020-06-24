#!/bin/bash

# setup: create a storj bucket and upload files via uplink
uplink --access=$acc mb sj://test
uplink --access=$acc cp test.txt sj://test
uplink --access=$acc cp test.txt sj://test/a/e

# setup: make local directory to mount to test-fs
mkdir -p mt/1

# setup: start test-fs and mount that local directory
./test-fs -access=$acc -bucket="test" -mountpoint=mt/1

# run tests
ls -lah mt/1
ls -lah mt/1/a
ls -lah mt/1/a/e
cat mt/1/a/e/test.txt
cat mt/1/test.txt

# setup: start test-fs and mount that local directory with a prefix
./test-fs -access=$acc -bucket="test" -mountpoint=mt/1 -prefix=a/e/

# run tests
cat mt/1/test.txt

# cleanup
uplink --access=$acc rm sj://test/a/e/test.txt
uplink --access=$acc rm sj://test/test.txt
uplink --access=$acc rb sj://test
