load_tools
==========

Tools for efficient and error-tolerant loading of character-delimited data into SciDB.

## Intro

load_tools is a collection of load-related User-Defined Operators and Functions. The key component is the two-phase split-parse design. The input file (or fs object) is read by the split() operator and converted into simple large string chunks as fast as possible. Those chunks are simultaneously redistributed among all the instances in the cluster. The operator parse() can then be run in parallel to process the chunks into a set number of string attributes. The result of parse may be stored, or converted directly into the final form. The framework offers the following features:

 * performance superior to loadcsv.py (based on preliminary benchmark results)
 * ingesting data into the system with a tolerance for errors 
 * ability to map errors to the exact line number in the original file
 * ability to correct errors in-database
 * a few helpful functions for casting and string manipulation
 * split() and parse() are independent; either operator can be repalced with a different piece of code
 * in the future, split() can be taught to read from multiple files on different instances at the same time

The only drawback of the approach is that it does not respect escaped or quoted delimiters, as doing so will necessarily reduce performance. We like the TSV format as it expressly forbits escaped or quoted delimiters. It is possible to implement a more sophisticated, slower versions of split and parse for those cases.

## Example
We will motivate with this malformed TSV file:
```
$ cat /tmp/testfile 
col1	col2	col3
"alex"	1	3.5
"b"ob"	2	4.8
jake	4.0
random		3.1	"extra stuff"
bill	abc	9
alice	4	not_a_number
```

### Split
Split reads from an absolute path (instance 0 only at the moment) and returns an array of 
```
<value:string> [source_instance_id = 0:*,1,0, chunk_no = 0:*,1,0]
```
Note, value is a large string that stores an entire block of the file, 1 million lines by default - in a single chunk. The number of lines per chunk and the delimiter character can be passed as optional parameters:
```
$ iquery -aq "split('/tmp/testfile', 'lines_per_chunk=2')"
{source_instance_id,chunk_no} value
{0,0} 'col1	col2	col3
"alex"	1	3.5'
{0,1} '"b"ob"	2	4.8
jake	4.0'
{0,2} 'random		3.1	"extra stuff"
bill	abc	9'
{0,3} 'alice	4	not_a_number'

$ iquery -aq "split('/tmp/testfile', 'lines_per_chunk=3', 'delimiter=\t')"
{source_instance_id,chunk_no} value
{0,0} 'col1	col2	col3
"alex"'
{0,1} '1	3.5
"b"ob"	2'
{0,2} '4.8
jake	4.0
random	'
{0,3} '3.1	"extra stuff"
bill	abc'
{0,4} '9
alice	4	not_a_number
'
```

### Parse
After the file is split, it can be parsed into a desired number of attributes, a0...aN - all nullable strings. The result array is
```
<a0:string null, a1:string null... aN:string null, error:string null> 
[source_instance_id=0:*,1,0, chunk_no=0:*,1,0, line_no=0:*,CS,0]"
```
The chunk size is passed as an argument and should match the lines per chunk used in the call to split(). The attribute "error" is always provided and populated with null, unless a particular line does not have the matching number of attributes:
```
$ iquery -aq "store(parse(split('/tmp/testfile', 'lines_per_chunk=2'), 'num_attributes=3', 'chunk_size=2'), tmp)"
{source_instance_id,chunk_no,line_no} a0,a1,a2,error
{0,0,0} 'col1','col2','col3',null
{0,0,1} '"alex"','1','3.5',null
{0,1,0} '"b"ob"','2','4.8',null
{0,1,1} 'jake','4.0',null,'short'
{0,2,0} 'random','','3.1','long	"extra stuff"'
{0,2,1} 'bill','abc','9',null
{0,3,0} 'alice','4','not_a_number',null
```
There are only two possible non-null error values: "long" and "short" as shown above. Note we've stored this result into a temporary array where we can perform some data correction. Also note, we ingested the header line of the file "col1,...". However, we can easily filter() it out.

### Where are the errors?
We can easily find all the erroneous lines by filtering on the error attribute. We can also map these cells to the original line number in the file by using this simple formula. Remember: the number "2" is the chunk size we used during the load:
```
$ iquery -aq "apply(filter(tmp, error is not null), original_line_number, 1+line_no+chunk_no*2)"
{source_instance_id,chunk_no,line_no} a0,a1,a2,error,original_line_number
{0,1,1} 'jake','4.0',null,'short',4
{0,2,0} 'random','','3.1','long	"extra stuff"',5
```

### dcast: error-tolerant casting
The supplied UDF dcast can be used to cast a string to a double, int64 or uint64 (at the moment). Note, the default casting in scidb will fail the query on first error. The second argument to dcast is the "default value" to use when the cast fails, usually a null or missing code. For example:
```
$ iquery -aq "project(apply(filter(tmp, not (line_no=0 and chunk_no=0)), da2, dcast(a2, double(missing(1)))), a2, da2)"
{source_instance_id,chunk_no,line_no} a2,da2
{0,0,1} '3.5',3.5
{0,1,0} '4.8',4.8
{0,1,1} null,null
{0,2,0} '3.1',3.1
{0,2,1} '9',9
{0,3,0} 'not_a_number',?1
```
Note, the value at {0,1,1} was not present in the file and is thus "null". The value {0,3,0} was not castable, and we decided to use ?1 to differentiate.

### Some string utilities
trim() can be used to remove all characters from a given list from the start and end of the given string:
```
$ iquery -aq "project(apply(filter(tmp, not (line_no=0 and chunk_no=0)), ta0, trim(a0, '\"')), a0, ta0)"
{source_instance_id,chunk_no,line_no} a0,ta0
{0,0,1} '"alex"','alex'
{0,1,0} '"b"ob"','b"ob'
{0,1,1} 'jake','jake'
{0,2,0} 'random','random'
{0,2,1} 'bill','bill'
{0,3,0} 'alice','alice'

$ iquery -aq "project(apply(filter(tmp, not (line_no=0 and chunk_no=0)), ta0, trim(a0, '\"b')), a0, ta0)"
{source_instance_id,chunk_no,line_no} a0,ta0
{0,0,1} '"alex"','alex'
{0,1,0} '"b"ob"','o'
{0,1,1} 'jake','jake'
{0,2,0} 'random','random'
{0,2,1} 'bill','ill'
{0,3,0} 'alice','alice'
```
For more serious processing, consult the regular expression substitution routine provided in https://github.com/paradigm4/superfunpack

codify() can be used to convert a given string to its ascii representation:
```
$ iquery -aq "project(apply(tmp, ca0, codify(a0)), a0, ca0)"
{source_instance_id,chunk_no,line_no} a0,ca0
{0,0,0} 'col1','99|111|108|49|0|'
{0,0,1} '"alex"','34|97|108|101|120|34|0|'
{0,1,0} '"b"ob"','34|98|34|111|98|34|0|'
{0,1,1} 'jake','106|97|107|101|0|'
{0,2,0} 'random','114|97|110|100|111|109|0|'
{0,2,1} 'bill','98|105|108|108|0|'
{0,3,0} 'alice','97|108|105|99|101|0|'
```
===
Hopefully more enhancements coming soon!
