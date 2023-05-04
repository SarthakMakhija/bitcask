# bitcask - Disk based Log Structured Hash Table Store
This is the golang implementation of Riak's [bitcask](https://riak.com/assets/bitcask-intro.pdf) paper. This project is for *educational* purposes only. The idea is to
provide a reference implementation of bitcask to help anyone interested in storage engines understand the basics of building a persistent key-value storage engine.

[![bitcask](https://github.com/SarthakMakhija/bitcask/actions/workflows/build.yml/badge.svg)](https://github.com/SarthakMakhija/bitcask/actions/workflows/build.yml)

![bitcask_keydir](https://user-images.githubusercontent.com/21108320/236152173-c48ec978-f1b3-4a6c-a31e-630af6e3bdce.png)

I have written a detailed blog on [bitcask](https://tech-lessons.in/blog/bitcask/).

# Features
- Support for `put`, `get`, `update` and `delete` operations
- Low latency for reads and writes
- Simple and easy to understand
- Configurable compaction
- Rich documentation

# Limitations
- The implementation does not support transactions
- The implementation does not support range queries
- The implementation does not support hint files
- RAM usage is high because all the keys are stored in an in-memory hashmap
- Too many open files handles at the OS end

# Idea

### Write operations
Every write operation (`put(key, value)`, `update(key,value)` and `delete(key)`) goes in an append-only data file.
At any moment, one file is "active" for writing. When that file meets a size threshold, it will be closed for writing, and a new active file will be created.
Once a file is closed for writing, it is considered immutable (or inactive) and will never be opened for writing again. However, it will still be used for reading.

All the entries in the data file follow a fixed structure:

| timestamp | key size      | value size | key | value     |
|-----------|------------|------------|-----|---------|

This implementation of bitcask uses 32 bits for the timestamp, 32 bits for the key size and 32 bits for the value size. Once an entry is written to the append-only data file, the key, along with its file metadata, is stored in an in-memory hashmap.
It stores the key and an `Entry` consisting of `FileId`, `Offset` and `EntryLength` as the value in the hashmap.

### Read operations
The `get` operation performs a lookup in the hashmap and gets an `Entry`.

If the `Entry` corresponding to the key is found, a read operation is performed in the file identified by the `fileId`. This read operation involves performing a `Seek` to the offset in the file and then reading the entire entry (`[]byte`) identified by the entry length. After the entry is read, it is decoded to get the value.

### Compaction
Every update and delete operation is also an append operation to a data file. This model may use up a lot of space over time, since we just write out new values without touching the old ones. A compaction process referred to as "merging" solves this. The merge process iterates over all non-active (i.e. immutable) files and produces as output a set of data files containing only the latest values of each present key.

# Documentation
The implementation has code comments to help readers understand the reasons behind various decisions and explain the working of the bitcask model.

Sample comment:

<img width="1800" src="https://user-images.githubusercontent.com/21108320/235490077-f47d3c12-d38f-4363-bea5-28cf5bddbba3.png">

# Reference
[bitcask](https://riak.com/assets/bitcask-intro.pdf)
