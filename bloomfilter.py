import math
import time
from datetime import datetime, timedelta
from math import ceil, log


# For hash functions see http://www.partow.net/programming/hashfunctions/index.html
# Author Arash Partow, CPL http://www.opensource.org/licenses/cpl1.0.php


def FNVHash(key):
    fnv_prime = 0x811C9DC5
    hash = 0
    for i in range(len(key)):
        hash *= fnv_prime
        hash ^= ord(key[i])
    return hash


def APHash(key):
    hash = 0xAAAAAAAA
    for i in range(len(key)):
        if ((i & 1) == 0):
            hash ^= ((hash << 7) ^ ord(key[i]) * (hash >> 3))
        else:
            hash ^= (~((hash << 11) + ord(key[i]) ^ (hash >> 5)))
    return hash


# copied from https://github.com/jaybaird/python-bloomfilter
def bloom_calculator(n, p):
    """
    Calculate the optimal bloom filter parameters for a given number of elements in filter (n) and false
    positive probability (p)
    """
    size = int(ceil((n * log(p)) / log(1.0 / (pow(2.0, log(2.0))))))
    hashes = int(log(2.0) * size / n)
    return size, hashes


class TimeSeriesBloomFilter(object):
    # todo: make it more clear how all this works
    # todo: create a helper function that calculates the total amount of memory stored

    def __init__(self, connection, bitvector_key, n=85001024, k=2, elements=None, probability=None, fill_max=0.75, **kwargs):
        self.time_resolution = kwargs.get('time_resolution', timedelta(minutes=1))
        self.time_limit = kwargs.get('time_limit', timedelta(minutes=10))
        self.time_limit_seconds = self.time_limit.days * 86400 + self.time_limit.seconds
        self.connection = connection
        self.bitvector_key = bitvector_key
        self.fill_max = fill_max
        self.elements = elements
        self.probability = probability
        self.n = n
        self.k = k
        if elements and probability:
            self.n, self.k = bloom_calculator(elements, probability)

    def most_current_filters(self, within, now):
        resolution_microseconds = (self.time_resolution.days * 86400 + self.time_resolution.seconds) * 1e6 + \
            self.time_resolution.microseconds

        within_microseconds = (within.days * 86400 + within.seconds) * 1e6 + within.microseconds

        # how many bloom filters will we need to iterate for this?
        num_filters = int(math.ceil(within_microseconds / resolution_microseconds))

        # figure out what the passed timestamp really is
        current_microtimestamp = time.mktime(now.timetuple()) * 1e6 + now.microsecond

        # get a datetime object of the 'current' filter
        block = resolution_microseconds * math.floor(current_microtimestamp / resolution_microseconds)
        block_now = datetime.fromtimestamp(block / 1e6)

        for x in xrange(num_filters):
            filter_date = block_now - x * self.time_resolution
            filter_bitvector_key = '%s|%s' % (self.bitvector_key, filter_date.isoformat())
            yield BloomFilter(self.connection, filter_bitvector_key, self.n, self.k, fill_max=self.fill_max)

    def add(self, key, **kwargs):
        within = kwargs.get('within', self.time_resolution)
        now = kwargs.get('now', datetime.now())

        # add to the current bloom filter
        for bloom_filter in self.most_current_filters(within=within, now=now):
            # we'll expire the bloom filter we're setting to after 'limit' + 1 seconds
            bloom_filter.add(key, timeout=self.time_limit_seconds + 1)

    def delete(self, key, **kwargs):
        within = kwargs.get('within', self.time_limit)
        now = kwargs.get('now', datetime.now())

        # delete from the time series bloomfilters
        for bloom_filter in self.most_current_filters(within=within, now=now):
            # in case of creating new filter when deleting, so check first
            if key in bloom_filter:
                bloom_filter.delete(key)

    def __contains__(self, key, **kwargs):
        # checks if this time series bloom filter has
        # contained an element within the last x minutes
        within = kwargs.get('within', self.time_limit)
        now = kwargs.get('now', datetime.now())

        for i, bloom_filter in enumerate(self.most_current_filters(within=within, now=now)):
            if key in bloom_filter:
                return True
        else:
            return False

    # lookup support for the 'within' parameter that we can't express in the magic __contains__
    contains = __contains__


class BloomFilter(object):

    def __init__(self, connection, bitvector_key, n=85001024, k=2, elements=None, probability=None, fill_max=0.75):
        # create a bloom filter based on a redis connection, a bitvector_key (name) for it
        # and the settings n & k, which dictate how effective it will be
        # - n is the amount of bits it will use, I have had success with 85001024 (500kiB)
        #   for 100k values. If you have fewer, you can get away with using fewer bits.
        #   in general, the more bits, the fewer false positives
        # - k is the number of hash derivations it uses, too many will fill up the filter
        #   too quickly, not enough will lead to many false positives

        self.connection = connection
        self.bitvector_key = bitvector_key
        self.fill_max = fill_max
        self.elements = elements
        self.probability = probability
        self.n = n
        self.k = k

        # test for is_full ever N adds
        self._add_count = 0
        self._add_count_check = 1000

        if elements and probability:
            self.n, self.k = bloom_calculator(elements, probability)

        self.init_redis(clear=False)

    def init_redis(self, clear=False):
        if clear:
            if self.connection.exists(self.bitvector_key):
                self.connection.delete(self.bitvector_key)
            self.connection.setbit(self.bitvector_key, self.n, 0)
        else:
            if not self.connection.exists(self.bitvector_key):
                self.connection.setbit(self.bitvector_key, self.n, 0)

    def __contains__(self, key):
        pipeline = self.connection.pipeline()
        for hashed_offset in self.calculate_offsets(key):
            pipeline.getbit(self.bitvector_key, hashed_offset)
        results = pipeline.execute()
        return all(results)

    def _add(self, key, pipeline, set_value=1):
        for hashed_offset in self.calculate_offsets(key):
            pipeline.setbit(self.bitvector_key, hashed_offset, set_value)

    def add(self, key, set_value=1, transaction=False, timeout=None, pipeline=None):
        # set bits for every hash to 1
        # sometimes we can use pipelines here instead of MULTI,
        # which makes it a bit faster
        pipeline = self.connection.pipeline(transaction=transaction)
        self._add(key, pipeline, set_value)

        if timeout is not None:
            pipeline.expire(self.bitvector_key, timeout)

        pipeline.execute()

    def extend(self, keys, set_value=1, transaction=True, timeout=None):
        ''' efficiently add a list of keys '''
        if not isinstance(keys, list):
            raise TypeError()

        pipeline = self.connection.pipeline(transaction=transaction)
        for key in keys:
            self._add(key, pipeline=pipeline, set_value=set_value)
        pipeline.execute()

        if self._add_count > self._add_count_check:
            self._add_count = 0
            if self.is_full:
                self.init_redis(clear=True)

    def delete(self, key):
        # delete is just an add with value 0
        # make sure the pipeline gets wrapped in MULTI/EXEC, so
        # that a deleted element is either fully deleted or not
        # at all, in case someone is checking __contains__ while
        # an element is being deleted
        self.add(key, set_value=0, transaction=True)

    def calculate_offsets(self, key):
        # we're using only two hash functions with different settings, as described
        # by Kirsch & Mitzenmacher: http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
        hash_1 = FNVHash(key)
        hash_2 = APHash(key)

        for i in range(self.k):
            yield (hash_1 + i * hash_2) % self.n

    @property
    def fill(self):
        ''' return the fraction of bit's currently filled '''
        return self.connection.bitcount(self.bitvector_key) / self.n

    @property
    def is_full(self):
        ''' does fill exceed fill max '''
        return self.fill > self.fill_max
