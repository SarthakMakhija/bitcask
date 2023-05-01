# bitcask - Disk based Log Structured Hash Table Store
This is golang based implementation of Riak's [bitcask](https://riak.com/assets/bitcask-intro.pdf) paper. This project is for educational purposes only. The idea is to 
provide a reference implementation of bitcask to help anyone interested in the area of storage engine understand the basics of building a persistent key-value storage engine.

[![bitcask](https://github.com/SarthakMakhija/bitcask/actions/workflows/build.yml/badge.svg)](https://github.com/SarthakMakhija/bitcask/actions/workflows/build.yml)

![bitcask](https://user-images.githubusercontent.com/21108320/235445730-4ed5e92c-b459-4e11-b7fd-6640251b4112.png)

# Features
- Low latency for reads and writes
- High throughput
- Simple and easy to understand
- Configurable compaction

# Limitations
- The implementation does not support transactions
- The implementation does not support range queries
- The implementation does not support hint files
- RAM usage is high because all the keys are stored in an in-memory hashmap

# Reference
[bitcask introduction](https://riak.com/assets/bitcask-intro.pdf)
