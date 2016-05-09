=================
bloomfilter-redis
=================

Standard & time series bloom filters, backed by Redis bit vectors.

This implementation is Python-only. If you're looking for (way) more speed, check out a C-based extension that uses hiredis at https://github.com/seomoz/pyreBloom

Overview
========

This is the little bloom filter we're using to filter unique views using redis.

It doesn't do anything special, but I didn't find any small and dependency-free bloom
filter written in Python that use Redis as their backend.

Time Series
========
If you're tracking users over time, and you want to answer the question "have we seen
this guy in the past 2 minutes", this is exactly right for you. For high-throughput
applications this is very space-effective. The total memory footprint is known before-
hand, and is based on the amount of history you want to save and the resolution.

You might track users in the past 2 minutes with a 10-second resolution using 12 bloom
filters. User hits are logged into the most recent bloom filter, and checking if you have
seen a user in the past 2 minutes will just go back through those 12 filters.

The finest resolutions possible are around 1ms. If you're pushing it to this limit you'll
have to take care of a bunch of things: Storing to and retrieving from Redis takes some
time. Timestamps aren't all that exact, especially when running on a virtual machine. If
you're using multiple machines, their clocks have to be perfectly in sync.


Quick Syntax
======
```python
  connection = redis.Redis()
  
  # directly initialize bloom filter
  # n = the ultimate bit length of the bloom
  # k = the number of hashes for each entry
  single = BloomFilter(connection=self.connection,
                       bitvector_key='test_bloomfilter',
                       n=1024,
                       k=4)
  
  # simple initialize bloom filter
  # calculates the n & k automatically
  # will reset the filter when ever single.fill > fill_max (0.75)
  single = BloomFilter(connection=self.connection,
                       bitvector_key='test_bloomfilter',
                       elements=10000,
                       probability=0.01,
                       fill_max=0.75)
  
  # add a single value to the bloom
  single.add('192.168.0.1')
  
  # add many values to the bloom
  # roughly 2-4 times as fast as looping through single.add
  single.extend(['192.168.0.2', '192.168.0.3'])
  
  
  assert('192.168.0.2' in single)
  assert('0.0.0.0' not in single)
  
  assert(not single.is_full)
  
  assert(single.fill < 0.01)

```


Quick Benchmarks
================

Quick benchmark for ballpark figures on a MacbookPro (2x 2.66GHz) with Python 2.7,
hiredis and Redis 2.9 (unstable). Each benchmark was run with k=4 hashes per key. Keys
are random strings of 10 chars length:

Big filter with fewer values:
filling bloom filter of 1024.00kB size with 10k values
adding 10000 values took 2.09s (4790 values/sec, 208.73 us/value)
correct: 100000 / false: 0 -> 0% false positives

Small filter with a lot of values:
filling bloom filter of 500.00kB size with 100k values
adding 100000 values took 22.30s (4485 values/sec, 222.96 us/value)
correct: 100000 / false: 3 -> 0.003% false positives

4 parallel Python processes:
filling bloom filter of 1024.00kB size with 2M values
adding 2000000 values took 214.69s (9316 values/sec, 429.38 us/value)
