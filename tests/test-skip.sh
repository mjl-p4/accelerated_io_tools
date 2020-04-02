#!/bin/bash

set -o errexit

DIR=`dirname $0`
TEST_OUT=$DIR/test-skip.out
rm -f $TEST_OUT  # in case it exists from previous run

# Create test input files
rm -rf /tmp/load_tools_test
mkdir -p /tmp/load_tools_test
echo '%% Some text header
% some other text header
12138 3791 1290810
1 2 3' >> /tmp/load_tools_test/header-data.ssv

# Control Cases.  Within a given header:X, the two controls must match.
echo "Control Cases" >> $TEST_OUT
echo "header:0" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:0, attribute_delimiter:' ', num_attributes:3)" >> $TEST_OUT
echo "header:0, skip:'nothing'" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:0, attribute_delimiter:' ', num_attributes:3, skip:'nothing')" >> $TEST_OUT
echo "header:1" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:1, attribute_delimiter:' ', num_attributes:3)" >> $TEST_OUT
echo "header:1, skip:'nothing'" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:1, attribute_delimiter:' ', num_attributes:3, skip:'nothing')" >> $TEST_OUT
echo "header:2" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:2, attribute_delimiter:' ', num_attributes:3)" >> $TEST_OUT
echo "header:2, skip:'nothing'" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:2, attribute_delimiter:' ', num_attributes:3, skip:'nothing')" >> $TEST_OUT
echo "header:3" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:3, attribute_delimiter:' ', num_attributes:3)" >> $TEST_OUT
echo "header:3, skip:'nothing'" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:3, attribute_delimiter:' ', num_attributes:3, skip:'nothing')" >> $TEST_OUT

# Test Cases
echo "Test Cases" >> $TEST_OUT
echo "header:0, skip:'errors'" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:0, attribute_delimiter:' ', num_attributes:3, skip:'errors')" >> $TEST_OUT
echo "header:0, skip:'non-errors'" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:0, attribute_delimiter:' ', num_attributes:3, skip:'non-errors')" >> $TEST_OUT
echo "header:1, skip:'errors'" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:1, attribute_delimiter:' ', num_attributes:3, skip:'errors')" >> $TEST_OUT
echo "header:1, skip:'non-errors'" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:1, attribute_delimiter:' ', num_attributes:3, skip:'non-errors')" >> $TEST_OUT
echo "header:2, skip:'errors'" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:2, attribute_delimiter:' ', num_attributes:3, skip:'errors')" >> $TEST_OUT
echo "header:2, skip:'non-errors'" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:2, attribute_delimiter:' ', num_attributes:3, skip:'non-errors')" >> $TEST_OUT
echo "header:3, skip:'errors'" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:3, attribute_delimiter:' ', num_attributes:3, skip:'errors')" >> $TEST_OUT
echo "header:3, skip:'non-errors'" >> $TEST_OUT
iquery -otsv -aq "aio_input('/tmp/load_tools_test/header-data.ssv', header:3, attribute_delimiter:' ', num_attributes:3, skip:'non-errors')" >> $TEST_OUT

# Check results
diff -rub $TEST_OUT $DIR/test-skip.expected

# Cleanup
rm -f $TEST_OUT
rm -rf /tmp/load_tools_test
