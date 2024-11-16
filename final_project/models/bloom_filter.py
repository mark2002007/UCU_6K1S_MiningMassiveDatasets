import ast
import os
import math


class BloomFilter: # https://en.wikipedia.org/wiki/Bloom_filter
    def __init__(self, size: int = None, hash_count: int = None, capacity: int = None, false_positive_rate: float = None):
        # Ensure we have either (capacity, false_positive_rate) or (size, hash_count)
        if capacity is not None and false_positive_rate is not None and (size is None or hash_count is None):
            self.size = self._calculate_optimal_size(capacity, false_positive_rate)
            self.hash_count = self._calculate_optimal_hash_count(self.size, capacity)
            self.capacity = capacity
            self.false_positive_rate = false_positive_rate
        elif size is not None and hash_count is not None and (capacity is None or false_positive_rate is None):
            self.size = size
            self.hash_count = hash_count
            self.capacity = self._calculate_capacity(self.size, self.hash_count)
            self.false_positive_rate = self._calculate_false_positive_rate(self.size, self.capacity)
        else:
            raise ValueError("Must provide either (capacity, false_positive_rate) or (size, hash_count)")

        self.bit_array = [0] * self.size
        self.inserted_count = 0

    @staticmethod
    def _calculate_optimal_size(capacity, false_positive_rate): #
        size = int(-capacity * math.log(false_positive_rate) / (math.log(2) ** 2))
        return max(1, size)

    @staticmethod
    def _calculate_optimal_hash_count(size, capacity):  # 
        hash_count = int((size / capacity) * math.log(2))
        return max(1, hash_count)
    
    @staticmethod
    def _calculate_capacity(size, hash_count):
        capacity = int((size / hash_count) * math.log(2))  # int(-size * math.log(false_positive_rate) / (math.log(2) ** 2))
        return max(1, capacity)
    
    @staticmethod
    def _calculate_false_positive_rate(size, capacity):
        fpr = math.exp(-size/capacity*math.log(2) ** 2)  # (1 - math.exp(-hash_count * capacity / size)) ** hash_count
        return fpr

    def _get_hash_values(self, item):
        # Use two different hash functions
        hash1 = hash(str(item))
        hash2 = hash(str(item) + '0')
        
        for i in range(self.hash_count):
            # Kirsch-Mitzenmacher optimization: Use linear combination of two hash functions
            yield (hash1 + i * hash2) % self.size

    def add(self, item):
        if self.inserted_count >= self.capacity:
            print("Warning: Bloom filter is at capacity")
            
        for bit_index in self._get_hash_values(item):
            self.bit_array[bit_index] = 1
        self.inserted_count += 1

    def lookup(self, item):
        return all(self.bit_array[bit_index] == 1 for bit_index in self._get_hash_values(item))

    def reset(self):
        self.bit_array = [0] * self.size
        self.inserted_count = 0
        
    def __repr__(self):
        return (f'BloomFilter(size={self.size}, hash_count={self.hash_count}, '
                f'capacity={self.capacity}, false_positive_rate={self.false_positive_rate:.4f}, inserted_count={self.inserted_count})')
    
    def save(self, filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as file:
            file.writelines([
                f"size={self.size}, hash_count={self.hash_count}, capacity={self.capacity}, false_positive_rate={self.false_positive_rate}, inserted_count={self.inserted_count}\n",
                str(self.bit_array)
            ])

    @staticmethod
    def load(filepath):
        with open(filepath, 'r') as file:
            lines = file.readlines()
        if len(lines) != 2:
            raise ValueError("Wrong file given")
        params = lines[0].split(', ')
        if len(params) != 5:
            raise ValueError("Wrong file given")
        size_parts = params[0].split('size=')
        hash_parts = params[1].split('hash_count=')
        capacity_parts = params[2].split('capacity=')
        fpr_parts = params[3].split('false_positive_rate=')
        count_parts = params[4].split('inserted_count=')
        if len(size_parts) != 2 or len(hash_parts) != 2 or len(count_parts) != 2 or len(capacity_parts) != 2 or len(fpr_parts) != 2:
            raise ValueError("Wrong file given")
        try:
            size = int(size_parts[1])
            hash_count = int(hash_parts[1])
            inserted_count = int(count_parts[1])
            fpr = float(fpr_parts[1])
            capacity = int(capacity_parts[1])
        except Exception as e:
            raise ValueError(f"Wrong file given, {e}")
        try:
            bit_array = ast.literal_eval(lines[1])
        except Exception as e:
            raise ValueError(f"Wrong file given, {e}")
        filter = BloomFilter(size = size, hash_count=hash_count)
        filter.capacity = capacity
        filter.false_positive_rate = fpr
        filter.inserted_count = inserted_count
        filter.bit_array = bit_array
        return filter


def test_bloom_filter_fpr(bf, test_size=100_000):
    for i in range(bf.capacity):
        bf.add(str(i))
    false_positives = sum(1 for i in range(bf.capacity, bf.capacity + test_size) 
                         if bf.lookup(str(i)))    
    actual_fpr = false_positives / test_size
    return actual_fpr


if __name__ == '__main__':
    capacity = 100
    target_fpr = 0.01
    bf = BloomFilter(capacity=capacity, false_positive_rate=target_fpr)
    actual_fpr = test_bloom_filter_fpr(bf)
    bf.reset()

    print(f"Bloom Filter parameters: {bf}")
    print(f"Target FPR: {target_fpr:.6f}")
    print(f"Actual FPR: {actual_fpr:.6f}")
