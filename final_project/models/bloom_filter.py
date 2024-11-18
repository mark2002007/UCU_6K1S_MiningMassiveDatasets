# import ast
# import os
# import math

# import pyspark.sql.functions as F
# import pyspark.sql.types as T
# from pyspark.sql.functions import monotonically_increasing_id, row_number
# from pyspark.sql import Window, Row

# import logging

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# class BloomFilter: # https://en.wikipedia.org/wiki/Bloom_filter
#     def __init__(self, size: int = None, hash_count: int = None, capacity: int = None, false_positive_rate: float = None):
#         # Ensure we have either (capacity, false_positive_rate) or (size, hash_count)
#         if capacity is not None and false_positive_rate is not None and (size is None or hash_count is None):
#             self.size = self._calculate_optimal_size(capacity, false_positive_rate)
#             self.hash_count = self._calculate_optimal_hash_count(self.size, capacity)
#             self.capacity = capacity
#             self.false_positive_rate = false_positive_rate
#         elif size is not None and hash_count is not None and (capacity is None or false_positive_rate is None):
#             self.size = size
#             self.hash_count = hash_count
#             self.capacity = self._calculate_capacity(self.size, self.hash_count)
#             self.false_positive_rate = self._calculate_false_positive_rate(self.size, self.capacity)
#         else:
#             raise ValueError("Must provide either (capacity, false_positive_rate) or (size, hash_count)")

#         self.bit_array = [0] * self.size
#         self.inserted_count = 0

#     @staticmethod
#     def _calculate_optimal_size(capacity, false_positive_rate): #
#         size = int(-capacity * math.log(false_positive_rate) / (math.log(2) ** 2))
#         return max(1, size)

#     @staticmethod
#     def _calculate_optimal_hash_count(size, capacity):  # 
#         hash_count = int((size / capacity) * math.log(2))
#         return max(1, hash_count)
    
#     @staticmethod
#     def _calculate_capacity(size, hash_count):
#         capacity = int((size / hash_count) * math.log(2))  # int(-size * math.log(false_positive_rate) / (math.log(2) ** 2))
#         return max(1, capacity)
    
#     @staticmethod
#     def _calculate_false_positive_rate(size, capacity):
#         fpr = math.exp(-size/capacity*math.log(2) ** 2)  # (1 - math.exp(-hash_count * capacity / size)) ** hash_count
#         return fpr

#     def _get_hash_values(self, item):
#         # Use two different hash functions
#         hash1 = hash(str(item))
#         hash2 = hash(str(item) + '0')
        
#         for i in range(self.hash_count):
#             # Kirsch-Mitzenmacher optimization: Use linear combination of two hash functions
#             yield (hash1 + i * hash2) % self.size

#     def add(self, item):
#         if self.inserted_count >= self.capacity:
#             print("Warning: Bloom filter is at capacity")
            
#         for bit_index in self._get_hash_values(item):
#             self.bit_array[bit_index] = 1
#         self.inserted_count += 1

#     def lookup(self, item):
#         return all(self.bit_array[bit_index] == 1 for bit_index in self._get_hash_values(item))

#     def reset(self):
#         self.bit_array = [0] * self.size
#         self.inserted_count = 0
        
#     def __repr__(self):
#         return (f'BloomFilter(size={self.size}, hash_count={self.hash_count}, '
#                 f'capacity={self.capacity}, false_positive_rate={self.false_positive_rate:.4f}, inserted_count={self.inserted_count})')
    
#     def save(self, filepath):
#         os.makedirs(os.path.dirname(filepath), exist_ok=True)
#         with open(filepath, 'w') as file:
#             file.writelines([
#                 f"size={self.size}, hash_count={self.hash_count}, capacity={self.capacity}, false_positive_rate={self.false_positive_rate}, inserted_count={self.inserted_count}\n",
#                 str(self.bit_array)
#             ])

#     @staticmethod
#     def load(filepath):
#         with open(filepath, 'r') as file:
#             lines = file.readlines()
#         if len(lines) != 2:
#             raise ValueError("Wrong file given")
#         params = lines[0].split(', ')
#         if len(params) != 5:
#             raise ValueError("Wrong file given")
#         size_parts = params[0].split('size=')
#         hash_parts = params[1].split('hash_count=')
#         capacity_parts = params[2].split('capacity=')
#         fpr_parts = params[3].split('false_positive_rate=')
#         count_parts = params[4].split('inserted_count=')
#         if len(size_parts) != 2 or len(hash_parts) != 2 or len(count_parts) != 2 or len(capacity_parts) != 2 or len(fpr_parts) != 2:
#             raise ValueError("Wrong file given")
#         try:
#             size = int(size_parts[1])
#             hash_count = int(hash_parts[1])
#             inserted_count = int(count_parts[1])
#             fpr = float(fpr_parts[1])
#             capacity = int(capacity_parts[1])
#         except Exception as e:
#             raise ValueError(f"Wrong file given, {e}")
#         try:
#             bit_array = ast.literal_eval(lines[1])
#         except Exception as e:
#             raise ValueError(f"Wrong file given, {e}")
#         filter = BloomFilter(size = size, hash_count=hash_count)
#         filter.capacity = capacity
#         filter.false_positive_rate = fpr
#         filter.inserted_count = inserted_count
#         filter.bit_array = bit_array
#         return filter
    
#     def get_serializable_state(self):
#         """
#         Serialize the state of the Bloom filter for broadcasting.
#         """
#         return {
#             "size": self.size,
#             "hash_count": self.hash_count,
#             "capacity": self.capacity,
#             "false_positive_rate": self.false_positive_rate,
#             "bit_array": self.bit_array,
#             "inserted_count": self.inserted_count
#         }

#     @staticmethod
#     def load_from_state(state):
#         """
#         Reconstruct a Bloom filter from the serialized state.
#         """
#         bloom_filter = BloomFilter(size=state["size"], hash_count=state["hash_count"])
#         bloom_filter.capacity = state["capacity"]
#         bloom_filter.false_positive_rate = state["false_positive_rate"]
#         bloom_filter.bit_array = state["bit_array"]
#         bloom_filter.inserted_count = state["inserted_count"]
#         return bloom_filter


# class BloomFilterBasedModel:
#     def __init__(self, spark_session, fpr: float = 0.1):
#         self.bloom_filter = None
#         self.fpr = fpr
#         self.trained = False
#         self.spark_session = spark_session
    
#     def fit(self, train_df):
#         labels_list = train_df.filter(F.col("label") == 1).select('user').distinct().rdd.map(lambda r: r[0]).collect()

#         self.bloom_filter = BloomFilter(capacity=len(labels_list), false_positive_rate=self.fpr)
#         for username in labels_list:
#             self.bloom_filter.add(username)

#         self.trained = True

#     def predict(self, df, output_pred_col: str = "prediction"):
#         if not self.trained:
#             raise ValueError("Model is not trained")
        
#         # Broadcast the Bloom Filter
#         bloom_filter_broadcast = self.spark_session.sparkContext.broadcast(self.bloom_filter)

#         # Define a UDF for Bloom Filter lookup
#         def bloom_lookup(username):
#             return int(bloom_filter_broadcast.value.lookup(username))
        
#         bloom_lookup_udf = F.udf(bloom_lookup, T.IntegerType())

#         # Apply the UDF to add the prediction column
#         final_df = df.withColumn(output_pred_col, bloom_lookup_udf(F.col("user")))

#         return final_df


#     # def predict(self, df, output_pred_col: str = "prediction"):
#     #     if not self.trained:
#     #         raise ValueError("Model is not trained")
        
#     #     # usernames = df.select("user").rdd.map(lambda r: r[0]).collect()

#     #     # predictions = [int(self.bloom_filter.lookup(username)) for username in usernames]

#     #     # b = self.spark_session.createDataFrame([(p,) for p in predictions], [output_pred_col])

#     #     # df = df.withColumn("row_idx", monotonically_increasing_id())
#     #     # b = b.withColumn("row_idx", monotonically_increasing_id())

#     #     # final_df = df.join(b, df.row_idx == b.row_idx).\
#     #     #             drop("row_idx")

#     #     return final_df
    
#     def save(self, path):
#         if not self.trained:
#             raise ValueError("Model is not trained")
#         self.bloom_filter.save(path)

#     @staticmethod
#     def load(spark_context, path):
#         assert os.path.exists(path)

#         model = BloomFilterBasedModel(spark_context, 0)
#         model.bloom_filter = BloomFilter.load(path)
#         model.trained = True

#         return model

import ast
import os
import math
import pyspark.sql.functions as F
import pyspark.sql.types as T
import numpy as np
from pyspark.ml.linalg import DenseVector


class BloomFilter:
    def __init__(self, size: int = None, hash_count: int = None, capacity: int = None, 
                 false_positive_rate: float = None):
        # If a Spark session is provided, we are working with a model-based approach
        self.trained = False
        
        # Initialize filter properties based on provided values
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

        self.bit_array = DenseVector([0] * self.size)
        self.inserted_count = 0
        
    @staticmethod
    def _calculate_optimal_size(capacity, false_positive_rate): 
        size = int(-capacity * math.log(false_positive_rate) / (math.log(2) ** 2))
        return max(1, size)

    @staticmethod
    def _calculate_optimal_hash_count(size, capacity): 
        hash_count = int((size / capacity) * math.log(2))
        return max(1, hash_count)

    @staticmethod
    def _calculate_capacity(size, hash_count):
        capacity = int((size / hash_count) * math.log(2))
        return max(1, capacity)

    @staticmethod
    def _calculate_false_positive_rate(size, capacity):
        fpr = math.exp(-size / capacity * math.log(2) ** 2)
        return fpr

    def _get_hash_values(self, item):
        hash1 = hash(str(item))
        hash2 = hash(str(item) + '0')
        
        for i in range(self.hash_count):
            yield (hash1 + i * hash2) % self.size

    def add(self, item):
        if self.inserted_count >= self.capacity:
            print("Warning: Bloom filter is at capacity")
        bit_array_np = np.array(self.bit_array.toArray())
        for bit_index in self._get_hash_values(item):
            bit_array_np[bit_index] = 1  # Update the bit
        self.bit_array = DenseVector(bit_array_np)
        self.inserted_count += 1

    def lookup(self, item):
        return all(self.bit_array[bit_index] == 1 for bit_index in self._get_hash_values(item))

    def reset(self):
        self.bit_array = DenseVector([0] * self.size)
        self.inserted_count = 0

    def save(self, filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as file:
            file.writelines([
                f"size={self.size}, hash_count={self.hash_count}, capacity={self.capacity}, false_positive_rate={self.false_positive_rate}, inserted_count={self.inserted_count}\n",
                str(self.bit_array.toArray())
            ])

    @staticmethod
    def load(filepath):
        with open(filepath, 'r') as file:
            lines = file.readlines()
        if len(lines) != 2:
            raise ValueError("Wrong file given")
        params = lines[0].split(', ')
        size = int(params[0].split('=')[1])
        hash_count = int(params[1].split('=')[1])
        inserted_count = int(params[4].split('=')[1])
        fpr = float(params[3].split('=')[1])
        capacity = int(params[2].split('=')[1])
        bit_array = ast.literal_eval(lines[1])
        bit_array = DenseVector(bit_array)

        filter = BloomFilter(size=size, hash_count=hash_count)
        filter.capacity = capacity
        filter.false_positive_rate = fpr
        filter.inserted_count = inserted_count
        filter.bit_array = bit_array
        filter.trained = True
        return filter

    # New methods for model-based usage
    def fit(self, train_df):
        # if self.spark_session is None:
        #     raise ValueError("Spark session is required for model-based operations.")
        
        labels_list = train_df.filter(F.col("label") == 1).select('user').distinct().rdd.map(lambda r: r[0]).collect()

        # Initialize Bloom Filter
        self.capacity = len(labels_list)
        self.size = self._calculate_optimal_size(self.capacity, self.false_positive_rate)
        self.hash_count = self._calculate_optimal_hash_count(self.size, self.capacity)

        self.bit_array = DenseVector([0] * self.size)
        self.inserted_count = 0

        # Add each username to the filter
        for username in labels_list:
            self.add(username)

        self.trained = True

    def predict(self, df, output_pred_col="prediction"):
        if not self.trained:
            raise ValueError("Model is not trained")

        # Define a UDF for Bloom Filter lookup
        def bloom_lookup(username):
            return int(self.lookup(username))

        bloom_lookup_udf = F.udf(bloom_lookup, T.IntegerType())

        # Apply the UDF to add the prediction column
        final_df = df.withColumn(output_pred_col, bloom_lookup_udf(F.col("user")))

        return final_df


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
