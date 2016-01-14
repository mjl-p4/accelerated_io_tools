accelerated_io_tools
==========

A separate, privately kept library that is a significant improvement over the regular load_tools package. Exists as a separate .so that compiles for SciDB 15.7. The accelerated_io_tools and regular load_tools libraries cannot coexist on the same installation; the user must load one or the other. The accelerated .so is superior in every way.

The old split and parse operators are still available and unchanged -- if things go wrong, those can be used.

## aio_input
This operator replaces split() and parse().
Example ingest from a single file:
```
$ iquery -anq "aio_input('/tmp/foo.tsv', 'num_attributes=2')"
```

Example CSV ingest from multiple files:
```
$ iquery -anq "store(
 aio_input(
  'paths=/tmp/foo.tsv;/tmp/foo2.tsv', 
  'instances=1;2', 
  'num_attributes=4',
  'attribute_delimiter=,',
  'split_on_dimension=1',
  'header=1'
 ), 
 temp
)"
```

#### Formally:
All parameters are supplied as `key=value` strings. The `num_attributes` as well as `path` or `paths` must always be specified:
```
aio_input('parameter=value', 'parameter2=value2;value3',...)
```

#### Load from one or multiple files:
* `path=/path/to/file`: the absolute path to load from. Assumed to be on instance 0, if set. If the operator encounters a string without '=' it uses that as path.
* `paths=/path/to/file1;/path/to/file2`: semicolon-seprated list of paths for loading from multiple fs devices.

If `paths` is used, then `instances` must be used to specify the loading instances:
* `instances=0;1;...`: semicolon-separated list of instance ids, in the same order as "paths". By default, the file will be read from instance 0.
  
#### File format settings:
* `num_attributes=N`: number of columns in the file (at least on the majority of the lines). Required.
* `header=H`: an integer number of lines to skip from the file;  if "paths" is used, applies to all files. Default is 0.
* `line_delimiter=L`: a character that separates the lines (cells) of the file; values of `\t` `\r` `\n` and ` ` are also supported. Default is `\n`.
* `attribute_delimiter=A`: a character that separates the columns (attributes) of the file; values of `\t` `\r` `\n` and ` ` are also supported. Default is `\t`.

#### Splitting on dimension:
* `split_on_dimension=<0/1>`: a flag that determines whether the file columns are placed in SciDB attributes, or cells along an extra dimension. Default is 0 (create attributes).

#### Tuning settings:
* `buffer_size=B`: the units into which the loaded file(s) are initially split when first redistributed across the cluster, specified in bytes; default is 8MB.
* `chunk_size=C`: the chunk size along the third dimension of the result array. Should not be required often as the `buffer_size` actually controls how much data goes in each chunk. Default is 10,000,000. If `buffer_size` is set, this value is automatically changed to equal `buffer_size` as an overestimate.

#### Returned array:
If `split_on_dimension=0` (default), the schema of the returned array is as follows:
```
 <a0:string null, a1:string null, ... aN-1: string null, error:string null>
 [tuple_no           = 0: *,                 CS,   0,
  dst_instance_id    = 0: NUM_INSTANCES-1,   1,    0,
  source_instance_id = 0: NUM_INSTANCES-1,   1,    0]
```
Where `N` is the specified `num_attributes` value, `CS` is the chunk size (10M default; see above) and `NUM_INSTANCES` is the number of SciDB instances in the cluster. The error attribute is null, unless the particular line in the file had a number of tokens not equal to `N`, in which case the error attribute is set to either 'short' or 'long ' followed by the leftover line. In the case of a short line, the absent attributes are set to null. 
 
If `split_on_dimension=1` the attributes are populated along a fourth dimension like so:
```
 <a:string null>
 [tuple_no           = 0: *,                 CS,   0,
  dst_instance_id    = 0: NUM_INSTANCES-1,   1,    0,
  source_instance_id = 0: NUM_INSTANCES-1,   1,    0,
  attribute_no       = 0: N,                 N+1,  0]
```
The slice of the array at attribute_no=N shall contain the error attribute, populated as above.
 
Other than `attribute_no` (when `split_on_dimension=1`) the dimensions are not intended to be used in queries. The `source_instance_id` matches the instance(s) reading the data; the `dst_instance_id` is assigned in a round-robin fashion to successive blocks from the same source. The `tuple_no` starts at 0 for each `{dst_instance_id, source_instance_id}` pair and is populated densely within the block. However, each new block starts a new chunk. 

## aio_save
This operator replaces the existing save functionality, for binary and tab-delimited formats. 
Example save to a binary file:
```
iquery -anq "aio_save( bar, '/tmp/bar.out', 'format=(int64, double null, string null)')"
```

Example save to two TSV files:
```
iquery -anq "aio_save(
 filter(bar, a>3),
 'paths=/tmp/bar1.tsv;/tmp/bar2.tsv',
 'instances=0;16',
 'format=tdv'
)
```

#### Formally:
```
aio_save(array, 'parameter1=value1', 'parameter2=value2',...)
```
The `path` or `paths` must always be specified.

#### Save to one or more files:
By default, the file is saved to a path on node 0 by instance 0. You can distribute the IO and network load by simultaneously writing data to multiple FS devices from several different instances (one instance per path).
* `path=/path/to/file`: the location to save the file; required. If the operator enocunters a string parameter without '=', it assumes that to be the path.
* `paths=/path1;/path2;..`: multiple file paths for saving from different instances, separated by semicolon. Must be specified along with "instances" and have an equal number of terms. Either `path` or `paths` must be specified, but not both.
* `instance=I`: the instance to save the file on. Default is 0.
* `instances=0;1;..`: multiple instance ID's for saving from different instances, separated by semicolon. Must be specified along with `paths` and have an equal number of unique terms. Either `instance` or `instances` must be specified, but not both.

#### Other settings:
* `format=F`: the format string, may be either `tdv` (token-delimited values) or a scidb-style binary format spec like `(int64, double null,...)`.
* `attributes_delimiter=A`: the character to write between array attributes. Default is a tab. applies when fomat is set to `tdv`. 
* `line_delimiter=L`: the character to write between array cells. Default is a newline. applies when format is set to `tdv`.
* `cells_per_chunk=C`: the number of array cells to place in a chunk before saving to disk. Default is 1,000,000.

#### Returned array:
The schema is always `<val:string null> [chunk_no=0:*,1,0, source_instance_id=0:*,1,0]`. The returned array is always empty as the operator's objective is to export the data.

Note: the order of the returned data is arbitrary by default. If the client requires data in specific order, they must:
 0. save from a single instance
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
