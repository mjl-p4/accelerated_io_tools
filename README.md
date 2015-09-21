accelerated_load_tools
==========

A separate, privately kept library that is superior to https://github.com/paradigm4/load_tools
accelerated_load_tools is a separate .so that compiles for SciDB 15.7 and 14.12. The accelerated_load_tools and regular load_tools so files cannot coexist on the same installation; the user must load one or the other. The accelerated .so is superior in every way.

The old split and parse operators are still available and unchanged -- if things go wrong, those can be used.

The old form parse(split(...)) is replaced by the new form proto_load(...). The parameters to proto_load are mostly a union of the split and parse parameters. There is no longer a "lines_per_chunk" setting because proto_load splits data in evenly-sized blocks, 8MB by default. The chunk_size setting is also less relevant - proto_load uses a heuristic to pick the chunk size; a user override shouldn't be needed. Just as the recent split, proto_load can read from one file or multiple files at the same time.

Example: loading from a single file with buffer_size set to 20MB:
```
$ iquery -aq "proto_load('/tmp/foo.tsv', 'num_attributes=2', 'buffer_size=20000000')"
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
