accelerated_load_tools
==========

A separate, privately kept library that is superior to https://github.com/paradigm4/load_tools
accelerated_load_tools is a separate .so that compiles for SciDB 15.7 and 14.12. The accelerated_load_tools and regular load_tools so files cannot coexist on the same installation; the user must load one or the other. The accelerated so is superior in every way.

## Multi-file split support

Split now can ingest from multiple files at the same time. The possible syntax is as follows:
```
split('paths=/path/to/file1;/path/to/file2/...', 'instances=0;1...')
```
Where instance 0 will open /path/to/file1 and instance 1 will open /path/to/file2. The number of paths and instances must match; instances cannot be repeated; paths can be absolute or relative. There will be a query error if any of the files is not found.

An input-like syntax is also possible:
```
split('paths=relative/path', 'instances=-1')
```
Where the path will be relative to each instance's data directory. Instances won't error out if a file is not found.

## Faster parse operation

A lower-level code path. In our tests this was about 3-4x faster. Let us know how it goes for you.
