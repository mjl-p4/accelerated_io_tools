#!/bin/bash

## Run with: ./benchmark_arrow.sh <SIZE>
## SIZE is the sample data size in MB, default is 50MB

set -o errexit

DIR=`dirname $0`
IQ="iquery --afl --query"
F=/dev/shm/1

MB=$1
RE='^[0-9]+$'
if ! [[ $MB =~ $RE ]] ; then
   MB=50
fi

CNT=$((MB * 32768))            # MB * 1024 * 1024 / 8 / 4


echo "I. int64, int64, int64, int64"
echo "1. store"
$IQ "set no fetch;
     store(
       apply(
         build(
           <z:int64 not null>[i=1:$CNT],
           random()),
         y, int64(random()),
         x, int64(random()),
         w, int64(random())),
       foo)"

echo "2. summary"
$IQ "show(foo)"
$IQ "summarize(foo)"

echo "3. save"
time $IQ "save(foo, '$F', 0, '(int64,int64,int64,int64)')"
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)"

echo "4. aio_save(binary)"
time $IQ "aio_save(foo, '$F', 'format=(int64,int64,int64,int64)')"
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)"

echo "5. aio_save(arrow)"
time $IQ "aio_save(foo, '$F', 'format=arrow')"
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)"

echo "6. Arrow read"
time python -c "import pyarrow; pyarrow.open_stream('$F').read_pandas()"

echo "7. SciDB-Py fetch"
time python -c "import scidbpy; scidbpy.connect().arrays.foo.fetch(atts_only=True)"

echo "8. SciDB-Py fetch w/ Arrow"
time python -c "import scidbpy; scidbpy.connect().arrays.foo.fetch(atts_only=True, use_arrow=True)"

$IQ "remove(foo)"


echo "II. int64, int64, int64, int64, string('')"
echo "1. store"
$IQ "set no fetch;
     store(
       apply(
         build(
           <z:int64 not null>[i=1:$CNT],
           random()),
         y, int64(random()),
         x, int64(random()),
         w, int64(random()),
         v, ''),
       foo)"

echo "2. summary"
$IQ "show(foo)"
$IQ "summarize(foo)"

echo "3. save"
time $IQ "save(foo, '$F', 0, '(int64,int64,int64,int64,string)')"
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)"

echo "4. aio_save(binary)"
time $IQ "aio_save(foo, '$F', 'format=(int64,int64,int64,int64,string)')"
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)"

echo "5. aio_save(arrow)"
time $IQ "aio_save(foo, '$F', 'format=arrow')"
sz=`stat --printf="%s" $F`
echo "$((sz / 1024 / 1024)) MB ($sz B)"

echo "6. Arrow read"
time python -c "import pyarrow; pyarrow.open_stream('$F').read_pandas()"

echo "7. SciDB-Py fetch"
time python -c "import scidbpy; scidbpy.connect().arrays.foo.fetch(atts_only=True)"

echo "8. SciDB-Py fetch w/ Arrow"
time python -c "import scidbpy; scidbpy.connect().arrays.foo.fetch(atts_only=True, use_arrow=True)"

$IQ "remove(foo)"
