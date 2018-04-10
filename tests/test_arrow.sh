#!/bin/bash

set -o errexit

DIR=`dirname $0`
TEST_OUT=$DIR/test_arrow.out
IQ="iquery --afl --query"
F=/tmp/1

function iq() {
    $IQ "$1" >> $TEST_OUT
}

# Load Python 2.7 (CentOS only)
source /opt/rh/python27/enable || :

: > $TEST_OUT                   # Reset output file


echo -e "\nI. Basic"
echo "1. Single chunk (int64)"
iq "aio_save(apply(build(<x:int64>[i=-10:10], i), y, iif(i%2=0, i, int64(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas())" \
    >> $TEST_OUT

echo "2. Multiple chunks (int64)"
iq "aio_save(apply(build(<x:int64>[i=1:100:0:20], i), y, iif(i%2=0, i, int64(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "3. double"
iq "aio_save(apply(build(<x:double>[i=1:20:0:4], i), y, iif(i%2=0, double(i), double(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "4. float"
iq "aio_save(apply(build(<x:float>[i=1:20:0:4], i), y, iif(i%2=0, float(i), float(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "5. int8"
iq "aio_save(apply(build(<x:double>[i=1:20:0:4], i), y, iif(i%2=0, int8(i), int8(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "6. int16"
iq "aio_save(apply(build(<x:double>[i=1:20:0:4], i), y, iif(i%2=0, int16(i), int16(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "7. int32"
iq "aio_save(apply(build(<x:double>[i=1:20:0:4], i), y, iif(i%2=0, int32(i), int32(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "8. int64"
iq "aio_save(apply(build(<x:double>[i=1:20:0:4], i), y, iif(i%2=0, int64(i), int64(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "9. string"
iq "aio_save(apply(build(<x:int64>[i=1:20:0:4], i), y, iif(i%2=0, string(i), string(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "10. uint8"
iq "aio_save(apply(build(<x:double>[i=1:20:0:4], i), y, iif(i%2=0, uint8(i), uint8(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "11. uint16"
iq "aio_save(apply(build(<x:double>[i=1:20:0:4], i), y, iif(i%2=0, uint16(i), uint16(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "12. uint32"
iq "aio_save(apply(build(<x:double>[i=1:20:0:4], i), y, iif(i%2=0, uint32(i), uint32(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "13. uint64"
iq "aio_save(apply(build(<x:double>[i=1:20:0:4], i), y, iif(i%2=0, uint64(i), uint64(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "14. int64, double, string"
iq "aio_save(apply(build(<x:int64>[i=1:20:0:4], i), y, iif(i%2=0, double(i), double(null)), z, iif(i%2=0, string(i), string(null))), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "15. Empty chunk"
iq "aio_save(filter(build(<x:int64>[i=1:20:0:4], i), x < 6 or x > 14), '$F', 'format=arrow')"
python -c "import pyarrow; print(pyarrow.open_stream('$F').read_all().to_pandas().sort_values('x').to_string(index=False))" \
    >> $TEST_OUT

echo "16. Different output sinks"
$IQ "aio_save(build(<x:int64>[i=0:0], i), 'console', 'format=arrow')" \
    >> $TEST_OUT
$IQ "aio_save(build(<x:int64>[i=0:0], i), 'stdout', 'format=arrow')" \
    >> $TEST_OUT
$IQ "aio_save(build(<x:int64>[i=0:0], i), 'stderr', 'format=arrow')" \
    >> $TEST_OUT


echo -e "\nII. Exceptions"

$IQ "aio_save(build(<x:bool>[i=0:0], i), '$F', 'format=arrow')" 2>&1 \
    |  sed --expression='s/ line: [0-9]\+//g'                        \
    |  grep --invert-match "Failed query id:" >> $TEST_OUT           \
    || echo "expected exception"

$IQ "aio_save(build(<x:char>[i=0:0], i), '$F', 'format=arrow')" 2>&1 \
    |  sed --expression='s/ line: [0-9]\+//g'                        \
    |  grep --invert-match "Failed query id:" >> $TEST_OUT           \
    || echo "expected exception"

$IQ "aio_save(build(<x:datetime>[i=0:0], i), '$F', 'format=arrow')" 2>&1 \
    |  sed --expression='s/ line: [0-9]\+//g'                            \
    |  grep --invert-match "Failed query id:" >> $TEST_OUT               \
    || echo "expected exception"

$IQ "aio_save(build(<x:datetimetz>[i=0:0], apply_offset(datetime(i), 0)), '$F', 'format=arrow')" 2>&1 \
    |  sed --expression='s/ line: [0-9]\+//g'                                                         \
    |  grep --invert-match "Failed query id:" >> $TEST_OUT                                            \
    || echo "expected exception"

echo -e "\nIII. int64, int64(null)"
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
time python -c "import pyarrow; print(len(pyarrow.open_stream('$F').read_pandas()))" \
     >> $TEST_OUT

echo "6. SciDB-Py fetch"
time python -c "import scidbpy; print(len(scidbpy.connect().arrays.foo.fetch(atts_only=True)))" \
     >> $TEST_OUT

echo "7. SciDB-Py fetch w/ Arrow"
time python -c "import scidbpy; print(len(scidbpy.connect().arrays.foo.fetch(atts_only=True, use_arrow=True)))" \
     >> $TEST_OUT

iq "remove(foo)"


echo -e "\nIV. int64, string(null)"
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
time python -c "import pyarrow; print(len(pyarrow.open_stream('$F').read_pandas()))" \
     >> $TEST_OUT

echo "6. SciDB-Py fetch"
time python -c "import scidbpy; print(len(scidbpy.connect().arrays.foo.fetch(atts_only=True)))" \
     >> $TEST_OUT

echo "7. SciDB-Py fetch w/ Arrow"
time python -c "import scidbpy; print(len(scidbpy.connect().arrays.foo.fetch(atts_only=True, use_arrow=True)))" \
     >> $TEST_OUT


iq "remove(foo)"


diff $TEST_OUT $DIR/test_arrow.expected
