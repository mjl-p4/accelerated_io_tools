#!/bin/bash

set -o errexit

DIR=`dirname $0`
TEST_OUT=$DIR/test_arrow.out
IQ="iquery --afl --query"
F=/tmp/1

function iq() {
    $IQ "$1" >> $TEST_OUT
}


: > $TEST_OUT
iq "aio_save(apply(build(<x:int64>[i=-9:9], i), y, iif(i%2=0, i, int64(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas())" \
    >> $TEST_OUT

iq "aio_save(apply(build(<x:int64>[i=0:99:0:20], i), y, iif(i%2=0, i, int64(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

$IQ "aio_save(build(<x:uint64>[i=0:0], i), '$F', 'format=arrow')" 2>&1 \
    |  sed --expression='s/ line: [0-9]\+//g'                              \
    |  grep --invert-match "Failed query id:" >> $TEST_OUT                 \
    || echo "expected exception"

$IQ "aio_save(build(<x:int64>[i=0:0], i), 'console', 'format=arrow')" \
    >> $TEST_OUT
$IQ "aio_save(build(<x:int64>[i=0:0], i), 'stdout', 'format=arrow')" \
    >> $TEST_OUT
$IQ "aio_save(build(<x:int64>[i=0:0], i), 'stderr', 'format=arrow')" \
    >> $TEST_OUT



echo "1. store"
time $IQ "set no fetch; store(apply(build(<x:int64>[i=1:32768000], i), y, x * x), foo)" \
     >> $TEST_OUT
iq "summarize(foo)"

echo "2. save"
time $IQ "save(foo, '$F', 0, '(int64, int64)')" \
     >> $TEST_OUT
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)"

echo "3. aio_save(binary)"
time $IQ "aio_save(foo, '$F', 'format=(int64, int64)')" \
     >> $TEST_OUT
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)"

echo "4. aio_save(arrow)"
time $IQ "aio_save(foo, '$F', 'format=arrow')" \
     >> $TEST_OUT
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)"

echo "5. SciDB-Py fetch"
time python -c "import scidbpy; scidbpy.connect().arrays.foo.fetch()"

echo "6. Arrow read"
time python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas())" \
     >> /dev/null

iq "remove(foo)"


diff $TEST_OUT $DIR/test_arrow.expected
