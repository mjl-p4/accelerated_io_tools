#!/bin/bash

set -o errexit

DIR=`dirname $0`
TEST_OUT=$DIR/test_binary.out
IQ="iquery --afl --query"
F=/tmp/1

function iq() {
    $IQ "$1" >> $TEST_OUT
}

: > $TEST_OUT                   # Reset output file


echo -e "\nI. Basic"
echo "1. Single chunk (int64)"
iq "aio_save(apply(build(<x:int64>[i=-10:10:0:21], i), y, iif(i%2=0, i, int64(null))), '$F', 'atts_only=0', 'format=(int64 null,int64 null,int64)')"
iq "input(<x:int64, y:int64, i:int64 not null>[j], '$F', 0, '(int64 null,int64 null,int64)')"

echo "2. Multiple chunks (int64)"
iq "aio_save(apply(build(<x:int64>[i=1:100:0:20], i), y, iif(i%2=0, i, int64(null))), '$F', 'atts_only=0', 'format=(int64 null,int64 null,int64)')"
iq "input(<x:int64, y:int64, i:int64 not null>[j], '$F', 0, '(int64 null,int64 null,int64)')"

echo "3. Multiple chunks, multiple dimensions (int64)"
iq "aio_save(
      apply(
        build(<x:int64>[i=1:30:0:10; j=1:10:0:5], i * j),
        y, iif(i%2=0, x, int64(null))),
      '$F',
      'atts_only=0',
      'format=(int64 null,int64 null,int64,int64)')"
iq "input(<x:int64, y:int64, i:int64 not null, j:int64 not null>[k], '$F', 0, '(int64 null,int64 null,int64,int64)')"

echo "4. bool"
iq "aio_save(apply(build(<x:bool not null>[i=1:20:0:4], i), y, iif(i%2=0, bool(i), bool(null))), '$F', 'atts_only=0', 'format=(bool,bool null,int64)')"
iq "input(<x:bool not null, y:bool, i:int64 not null>[j], '$F', 0, '(bool,bool null,int64)')"

echo "5. double"
iq "aio_save(
      apply(
        build(<x:double not null>[i=1:20:0:4], i / 10.),
        y, iif(i%2=0, double(i) / 10, double(null))),
      '$F',
      'atts_only=0',
      'format=(double,double null,int64)')"
iq "input(<x:double not null, y:double, i:int64 not null>[j], '$F', 0, '(double,double null,int64)')"

echo "6. float"
iq "aio_save(
      apply(
        build(<x:float not null>[i=1:20:0:4], i / 10.),
        y, iif(i%2=0, float(i) / 10, float(null))),
      '$F',
      'atts_only=0',
      'format=(float,float null,int64)')"
iq "input(<x:float not null, y:float, i:int64 not null>[j], '$F', 0, '(float,float null,int64)')"

echo "7. uint64"
iq "aio_save(
      apply(
        build(<x:uint64 not null>[i=1:20:0:4], i),
        y, iif(i%2=0, uint64(i), uint64(null))),
      '$F',
      'atts_only=0',
      'format=(uint64,uint64 null,int64)')"
iq "input(<x:uint64 not null, y:uint64, i:int64 not null>[j], '$F', 0, '(uint64,uint64 null,int64)')"

echo "8. int64, double, string"
iq "aio_save(
      apply(
        build(<x:int64>[i=1:20:0:4], i),
        y, iif(i%2=0, double(i) / 10, double(null)),
        z, iif(i%2=0, string(i), string(null))),
      '$F',
      'atts_only=0',
      'format=(int64 null,double null,string null,int64)')"
iq "input(<x:int64, y:double, z:string, i:int64 not null>[j], '$F', 0, '(int64 null,double null,string null,int64)')"

echo "9. Empty chunk"
iq "aio_save(filter(build(<x:int64>[i=1:20:0:4], i), x < 6 or x > 14), '$F', 'atts_only=0', 'format=(int64 null,int64)')"
iq "input(<x:int64, i:int64 not null>[j], '$F', 0, '(int64 null,int64)')"


echo -e "\nII. Exceptions"
echo "1. Incomplete format (missing)"
$IQ "aio_save(build(<x:int64 not null>[i=0:0], i), '$F', 'atts_only=0', 'format=(int64)')" 2>&1 \
    |  sed --expression='s/ line: [0-9]\+//g'                                                   \
    |  grep --invert-match "Failed query id:" >> $TEST_OUT                                      \
    || echo "expected exception"


echo "2. Incomplete format (extra)"
$IQ "aio_save(build(<x:int64 not null>[i=0:0], i), '$F', 'atts_only=0', 'format=(int64,int64,int64)')" 2>&1     \
    |  sed --expression='s/ line: [0-9]\+//g'                                                                   \
    |  grep --invert-match "Failed query id:" >> $TEST_OUT                                                      \
    || echo "expected exception"


echo "3. Incomplete format (wrong)"
$IQ "aio_save(build(<x:int64 not null>[i=0:0], i), '$F', 'atts_only=0', 'format=(int64,double)')" 2>&1  \
    |  sed --expression='s/ line: [0-9]\+//g'                                                           \
    |  grep --invert-match "Failed query id:" >> $TEST_OUT                                              \
    || echo "expected exception"


diff $TEST_OUT $DIR/test_binary.expected
