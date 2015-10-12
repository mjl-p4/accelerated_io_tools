accelerated_io_tools
==========

A separate, privately kept library that is superior to https://github.com/paradigm4/load_tools
accelerated_io_tools is a separate .so that compiles for SciDB 15.7 and 14.12. The accelerated_io_tools and regular load_tools so files cannot coexist on the same installation; the user must load one or the other. The accelerated .so is superior in every way.

The old split and parse operators are still available and unchanged -- if things go wrong, those can be used.

## aio_input

This is a new operator that replaces split and parse. The signature is as follows:
```
aio_input('parameter=value', 'parameter2=value2;value3',...)

One of the following path parameters must be specified:
  path=/path/to/file                    :: the absolute path to load from, will be read from instance 0 if set
                                        :: if the operator encounters a string without '=' it uses that as path
  paths=/path/to/file1;/path/to/file2   :: semicolon-seprated list of paths for loading from multiple fs devices

If "paths" is used, then "instances" must be used to specify the loading instances:
  instances=[0;1;...]                   :: semicolon-separated list of instance ids, in the same order as "paths"
                                        :: by default, the file will be read from instance 0
  
File-specific settings:
  num_attributes=N                      :: number of columns in the file (at least on the majority of the lines)
                                        :: required
  header=H                              :: an integer number of lines to skip from the file; 
                                        :: if "paths" is used, applies to all files
                                        :: default is 0
  line_delimiter=L                      :: a character that separates the lines (cells) of the file
                                        :: values of \t \r \n and [space] are also supported
                                        :: default is \n
  attribute_delimiter=A                 :: a character that separates the columns (attributes) of the file
                                        :: values of \t \r \n and [space] are also supported
                                        :: default is \t

Splitting on dimension:
 split_on_dimension=[0/1]              :: a flag that determines whether the columns are placed in SciDB 
                                       :: attributes, or cells along an extra dimension
                                       :: default is 0 (create attributes)

Tuning settings:
  buffer_size=B                         :: the units into which the loaded file(s) are initially split
                                        :: when first redistributes across the cluster
                                        :: specified in bytes; default is 8MB
  chunk_size=C                          :: the chunk size along the third dimension of the result array
                                        :: should not be required often as the buffer_size actually
                                        :: controls how much data goes in each chunk 
                                        :: default is 10,000,000. If buffer_size is set, this value is 
                                        :: automatically changed to buffer_size as an over_estimate.
Returned array:
 If split_on_dimension=0 (default), the schema is as follows
 <a0:string null, a1:string null, ... aN-1: string null, error:string null>
 [source_instance_id = 0:*,1,0,
  chunk_no           = 0:*,1,0,
  line_no             = 0:*, [CS], 0]
 Where N is the specified num_attributes value. The error attribute is null, unless the particular line
 in the file had a number of tokens not equal to N, in which case the error attribute is set to either
 'short' or 'long ' followed by the leftover line. In the case of a short line, the absent attributes 
 are set to null.
 
 If split_on_dimension=1 the attributes are populated along a fourth dimension:
 <a:string null>
 [source_instance_id = 0:*,1,0,
  chunk_no           = 0:*,1,0,
  line_no            = 0:*, [CS], 0,
  attribute_no       = 0:N, N+1, 0]
 The slice of the array at attribute_no=N shall contain the error attribute, populated as above.
```



Example: loading from a single file:
```
$ iquery -aq "proto_load('/tmp/foo.tsv', 'num_attributes=2')"
{source_instance_id,chunk_no,line_no} a0,a1,error
{0,0,0} 'a','0',null
{0,0,1} 'b','1',null
{0,0,2} 'c','2',null
{0,0,3} 'e','3',null
{0,0,4} 'f','4',null
{0,0,5} 'g','5',null
{0,0,6} 'h','6',null
```

Example: multi-path load:
```
$ iquery -aq "proto_load('paths=/tmp/foo.tsv;/tmp/foo2.tsv', 'instances=1;2', 'num_attributes=2')"
{source_instance_id,chunk_no,line_no} a0,a1,error
{1,0,0} 'a','0',null
{1,0,1} 'b','1',null
{1,0,2} 'c','2',null
{1,0,3} 'e','3',null
{1,0,4} 'f','4',null
{1,0,5} 'g','5',null
{1,0,6} 'h','6',null
{2,0,0} 'i','7',null
{2,0,1} 'j','8',null
{2,0,2} 'k','9',null
{2,0,3} 'l','10',null
{2,0,4} 'm','11',null
{2,0,5} 'n','12',null
{2,0,6} 'o','13',null
{2,0,7} 'p','14',null
{2,0,8} 'q','15',null
{2,0,9} 'r','16',null
```

## aio_save
This operator replaces the existing save functionality, for binary and tab-delimited formats. Works as follows:
```
aio_save(array, 'parameter1=value1', 'parameter2=value2',...)

Parameters are as follows:
 path=/path/to/file                 :: the location to save the file; required
                                    :: if the operator enocunters a string parameter without '=', it assumes 
                                    :: that to be the path
 instance=I                         :: the instance to save the file on. Default is 0.
 format=F                           :: the format string, may be either 'tdv' (token-delimited values) or a 
                                    :: scidb-style binary format spec like '(jnt64, double null,...)'
 attributes_delimiter=A             :: the character to write between array attributes. Default is a tab.
 line_delimiter=L                   :: the character to write between array cells. Default is a newline.
 cells_per_chunk=C                  :: the number of array cells to place in a chunk before saving to disk.
                                    :: Default is 1,000,000.
Returned array:
 The schema is always <val:string null> [chunk_no=0:*,1,0, source_instance_id=0:*,1,0]
 The returned array is always empty as the operator's objective is to export the data.
```

Example save to a binary file:
```
iquery -anq "save( bar, '/tmp/bar.out', 'format=(int64, double null, string null)')"
```

Note: the order of the returned data is arbitrary by default. If the client requires data in specific order, they must:
 1. add a sort operator
 2. make sure the sort chunk size (1M default) matches the aio_save cells_per_chunk (also 1M default)
 3. add an explicit sg between the sort and the save - with round-robin distribution

For example:
```
aio_save(
 _sg(
  sort(bar, attribute, 100000),
  1, -1
 ),
 '/path/to/file',
 'format=tdv',
 'cells_per_chunk=100000'
)
```
