import os
import scidbpy
import sys
import timeit


ar_names = ('bm_fix', 'bm_var')
ar_schemas = [None, None]
chunk_size = 100000
buffer_size = 3 * 1024 * 1024


def setup(mb, runs):
    cnt = mb * 1024 * 1024 / 8 / 4
    db = scidbpy.connect()

    print("""
Setup
===
Number of runs:    {:7d}
Target size:       {:10.2f} MB
Buffer size:       {:10.2f} MB
Chunk size:        {:7d}
Number of records: {:7d}""".format(
    runs,
    mb,
    buffer_size / 1024. / 1024,
    chunk_size,
    cnt))


    ar_name = ar_names[0]
    db.build('<z:int64 not null>[i=1:{}:0:{}]'.format(cnt, chunk_size),
             'random()'
             ).apply('y', 'int64(random())',
                     'x', 'int64(random())',
                     'w', 'int64(random())'
                     ).store(ar_name)
    ar_schemas[0] = db.arrays[ar_name].schema()
    scidb_bytes_fix = db.summarize(ar_name).project('bytes')['bytes'][0]
    chunks = db.summarize(
        ar_name, 'by_attribute:true').project(
            'chunks').fetch(atts_only=True, as_dataframe=False)[0][0][1]
    mem_bytes_fix = 0
    # mem_bytes_fix = db.scan(ar_name).fetch(atts_only=True,
    #                                        as_dataframe=False).nbytes
    db.aio_save(
        ar_name, "'/dev/shm/{}'".format(ar_name), "'format=arrow'").fetch()
    file_bytes_fix = os.path.getsize('/dev/shm/' + ar_name)

    print("""Number of chunks:  {:7d}

Fix Size Schema (int64 only)
---
SciDB size:        {:7.2f} MB
In-memory size:    {:7.2f} MB
File size:         {:7.2f} MB""".format(
    chunks,
    scidb_bytes_fix / 1024. / 1024,
    mem_bytes_fix / 1024. / 1024,
    file_bytes_fix / 1024. / 1024))


#     ar_name = ar_names[1]
#     db.build('<z:int64 not null>[i=1:{}:0:{}]'.format(cnt, chunk_size),
#              'random()'
#              ).apply('y', 'int64(random())',
#                      'x', 'int64(random())',
#                      'w', 'int64(random())',
#                      'v', "''"
#                      ).store(ar_name)
#     ar_schemas[1] = db.arrays[ar_name].schema()
#     scidb_bytes_var = db.summarize(ar_name).project('bytes')['bytes'][0]
#     mem_bytes_var = 0
#     # mem_bytes_var = db.scan(ar_name).fetch(atts_only=True,
#     #                                        as_dataframe=False).nbytes
#     db.aio_save(
#         ar_name, "'/dev/shm/{}'".format(ar_name), "'format=arrow'").fetch()
#     file_bytes_var = os.path.getsize('/dev/shm/' + ar_name)

#     print("""
# Variable Size Schema (int64 and string)
# ---
# SciDB size:        {:7.2f} MB
# In-memory size:    {:7.2f} MB
# File size:         {:7.2f} MB""".format(
#     scidb_bytes_var / 1024. / 1024,
#     mem_bytes_var / 1024. / 1024,
#     file_bytes_var / 1024. / 1024))

    return db


def save(mb, runs):
    setup = """
import scidbpy

db = scidbpy.connect()"""

    ar_name = ar_names[0]
    print("""
Save
===
Fix Size Schema (int64 only)
---""")
    fmt = '(int64,int64,int64,int64)'
    stmt = """
db.iquery("aio_save({ar_name}, '/dev/shm/{ar_name}', 'format={fmt}', 'buffer_size={buffer_size}')",
          fetch = True)""".format(
              ar_name=ar_name,
              fmt=fmt,
              buffer_size=buffer_size)
    rt = timeit.Timer(stmt=stmt, setup=setup).timeit(number=runs) / runs
    print("""\
Binary: {:6.2f} seconds {:6.2f} MB/second""".format(
      rt, mb / rt))

    fmt = 'arrow'
    stmt = """
db.iquery("aio_save({ar_name}, '/dev/shm/{ar_name}', 'format={fmt}', 'buffer_size={buffer_size}')",
          fetch = True)""".format(
              ar_name=ar_name,
              fmt=fmt,
              buffer_size=buffer_size)
    rt = timeit.Timer(stmt=stmt, setup=setup).timeit(number=runs) / runs
    print("""\
Arrow:  {:6.2f} seconds {:6.2f} MB/second""".format(
      rt, mb / rt))


#     ar_name = ar_names[1]
#     print("""
# Variable Size Schema (int64 and string)
# ---""")
#     fmt = '(int64,int64,int64,int64,string)'
#     stmt = """
# db.iquery("aio_save({ar_name}, '/dev/shm/{ar_name}', 'format={fmt}', 'buffer_size={buffer_size}')",
#           fetch = True)""".format(
#               ar_name=ar_name,
#               fmt=fmt,
#               buffer_size=buffer_size)
#     rt = timeit.Timer(stmt=stmt, setup=setup).timeit(number=runs) / runs
#     print("""\
# Binary: {:6.2f} seconds {:6.2f} MB/second""".format(
#       rt, mb / rt))

#     fmt = 'arrow'
#     stmt = """
# db.iquery("aio_save({ar_name}, '/dev/shm/{ar_name}', 'format={fmt}', 'buffer_size={buffer_size}')",
#           fetch = True)""".format(
#               ar_name=ar_name,
#               fmt=fmt,
#               buffer_size=buffer_size)
#     rt = timeit.Timer(stmt=stmt, setup=setup).timeit(number=runs) / runs
#     print("""\
# Arrow:  {:6.2f} seconds {:6.2f} MB/second""".format(
#       rt, mb / rt))


def download(mb, runs):
    setup = """
import scidbpy

db = scidbpy.connect()
schemas = (scidbpy.schema.Schema.fromstring('{}'),
           scidbpy.schema.Schema.fromstring('{}'))""".format(
               *ar_schemas)

    i = 0
    print("""
Download
===
Fix Size Schema (int64 only)
---""")
    stmt = """
db.iquery('scan({})',
          fetch=True,
          atts_only=True,
          schema=schemas[{}])""".format(ar_names[i], i)
    rt = timeit.Timer(stmt=stmt, setup=setup).timeit(number=runs) / runs
    print("""\
Binary: {:6.2f} seconds {:6.2f} MB/second""".format(
      rt, mb / rt))

    fmt = 'arrow'
    stmt = """
db.iquery('scan({})',
          fetch=True,
          use_arrow=True,
          atts_only=True,
          schema=schemas[{}])""".format(ar_names[i], i)
    rt = timeit.Timer(stmt=stmt, setup=setup).timeit(number=runs) / runs
    print("""\
Arrow:  {:6.2f} seconds {:6.2f} MB/second""".format(
      rt, mb / rt))


    i = 1
    print("""
Variable Size Schema (int64 and string)
---""")
    stmt = """
db.iquery('scan({})',
          fetch=True,
          atts_only=True,
          schema=schemas[{}])""".format(ar_names[i], i)
    rt = timeit.Timer(stmt=stmt, setup=setup).timeit(number=runs) / runs
    print("""\
Binary: {:6.2f} seconds {:6.2f} MB/second""".format(
      rt, mb / rt))

    fmt = 'arrow'
    stmt = """
db.iquery('scan({})',
          fetch=True,
          use_arrow=True,
          atts_only=True,
          schema=schemas[{}])""".format(ar_names[i], i)
    rt = timeit.Timer(stmt=stmt, setup=setup).timeit(number=runs) / runs
    print("""\
Arrow:  {:6.2f} seconds {:6.2f} MB/second""".format(
      rt, mb / rt))


def cleanup(db):
    for ar_name in ar_names:
        try:
            db.remove(ar_name)
        except:
            pass


if __name__ == "__main__":
    try:
        mb = int(sys.argv[1])
    except Exception:
        mb = 5                      # MB
    runs = 3

    db = setup(mb, runs)

    save(mb, runs)
    # download(mb, runs)

    cleanup(db)
