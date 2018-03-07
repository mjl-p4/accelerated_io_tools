#!/bin/bash

set -o errexit

DIR=`dirname $0`
TEST_OUT=$DIR/test_arrow.out
IQ="iquery --afl --query"
F=/tmp/1

function iq() {
    $IQ "$1" >> $TEST_OUT
}


: > $TEST_OUT                   # Reset output file

iq "aio_save(apply(build(<x:int64>[i=-10:10], i), y, iif(i%2=0, i, int64(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas())" \
    >> $TEST_OUT

iq "aio_save(apply(build(<x:int64>[i=1:100:0:20], i), y, iif(i%2=0, i, int64(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

iq "aio_save(apply(build(<x:int64>[i=1:20:0:4], i), y, iif(i%2=0, string(i), string(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

$IQ "aio_save(build(<x:uint64>[i=0:0], i), '$F', 'format=arrow')" 2>&1 \
    |  sed --expression='s/ line: [0-9]\+//g'                          \
    |  grep --invert-match "Failed query id:" >> $TEST_OUT             \
    || echo "expected exception"

$IQ "aio_save(build(<x:int64>[i=0:0], i), 'console', 'format=arrow')" \
    >> $TEST_OUT
$IQ "aio_save(build(<x:int64>[i=0:0], i), 'stdout', 'format=arrow')" \
    >> $TEST_OUT
$IQ "aio_save(build(<x:int64>[i=0:0], i), 'stderr', 'format=arrow')" \
    >> $TEST_OUT


echo "I. int64, int64(null)"
echo "1. store"
time $IQ "set no fetch; store(apply(build(<x:int64 not null>[i=1:3100000], i), y, iif(i%2=0, i * i, int64(null))), foo)" \
     >> $TEST_OUT
iq "summarize(foo)"

echo "2. save"
time $IQ "save(foo, '$F', 0, '(int64, int64 null)')" \
     >> $TEST_OUT
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)" \
     >> $TEST_OUT

echo "3. aio_save(binary)"
time $IQ "aio_save(foo, '$F', 'format=(int64, int64 null)')" \
     >> $TEST_OUT
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)" \
     >> $TEST_OUT

echo "4. aio_save(arrow)"
time $IQ "aio_save(foo, '$F', 'format=arrow')" \
     >> $TEST_OUT
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)" \
     >> $TEST_OUT

echo "5. Arrow read"
time python -c "import pyarrow; pyarrow.open_stream('$F').read_pandas()"

echo "6. SciDB-Py fetch"
time python -c "import scidbpy; scidbpy.connect().arrays.foo.fetch(atts_only=True)"

echo "7. SciDB-Py fetch w/ Arrow"
time python -c "import scidbpy; scidbpy.connect().arrays.foo.fetch(atts_only=True, use_arrow=True)"

iq "remove(foo)"


echo "II. int64, string(null)"
echo "1. store"
time $IQ "set no fetch; store(apply(build(<x:int64 not null>[i=1:2700000], i), y, iif(i%2=0, string(i * i), string(null))), foo)" \
     >> $TEST_OUT
iq "summarize(foo)"

echo "2. save"
time $IQ "save(foo, '$F', 0, '(int64, string null)')" \
     >> $TEST_OUT
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)" \
     >> $TEST_OUT

echo "3. aio_save(binary)"
time $IQ "aio_save(foo, '$F', 'format=(int64, string null)')" \
     >> $TEST_OUT
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)" \
     >> $TEST_OUT

echo "4. aio_save(arrow)"
time $IQ "aio_save(foo, '$F', 'format=arrow')" \
     >> $TEST_OUT
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)" \
     >> $TEST_OUT

echo "5. Arrow read"
time python -c "import pyarrow; pyarrow.open_stream('$F').read_pandas()"

echo "6. SciDB-Py fetch"
time python -c "import scidbpy; scidbpy.connect().arrays.foo.fetch(atts_only=True)"

echo "7. SciDB-Py fetch w/ Arrow"
time python -c "import scidbpy; scidbpy.connect().arrays.foo.fetch(atts_only=True, use_arrow=True)"


iq "remove(foo)"


diff $TEST_OUT $DIR/test_arrow.expected
