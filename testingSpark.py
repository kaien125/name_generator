import pandas as pd
import random
import string
import sys
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import rand 
import pyspark.sql.functions as F
from pyspark.sql.functions import length


try:
    bucket = sys.argv[1]
except IndexError:
    print("Please provide a bucket name")
    sys.exit(1)

spark = SparkSession.builder.appName("name_generator").getOrCreate()

first_name_fields = [StructField('first_name', StringType(), True)]
first_name_schema = StructType(first_name_fields)

last_name_fields = [StructField('last_name', StringType(), True)]
last_name_schema = StructType(last_name_fields)

full_name_fields = [StructField("last_name", StringType(), True),
                StructField("middle_name_1", StringType(), True),
                StructField("middle_name_2", StringType(), True),
                StructField("first_name", StringType(), True)]
full_name_schema = StructType(full_name_fields)

full_name = spark.createDataFrame([], full_name_schema)
first_name = spark.createDataFrame([], first_name_schema)
last_name = spark.createDataFrame([], last_name_schema)

first_name_path = f"gs://{bucket}/first_names.csv"
last_name_path = f"gs://{bucket}/last_name.csv"

files_read = []


first_name = (
    spark.read.format('csv')
    .load(first_name_path, schema=first_name_schema)
    .union(first_name)
)
            
files_read.append(first_name_path)

last_name = (
    spark.read.format('csv')
    .load(last_name_path, schema=last_name_schema)
    .union(last_name)
)
            
files_read.append(last_name_path)


    
        
if len(files_read) == 0:
    print('No files read')
    sys.exit(1)


# append last name to full_name and repeat
for i in range(10):    
    full_name = full_name.union(last_name)

# shuffle rows to break alphabetical order
full_name = full_name.orderBy(rand())


first_name_len = length(first_name.first_name)
full_name_len = length(full_name.full_name)

# fill random first_name
for i in range(full_name_len-1):
    full_name.first_name[i] = first_name.first_name[random.randrange(first_name_len-1)]
    print(i)

# fill random
# assume 1/20 of people have one middle name 
for i in range((full_name_len-1)//3):
    full_name.middle_name_1[i] = first_name.first_name[random.randrange(first_name_len-1)]
    print(i)

# fill random
# assume 1/100 of people have second middle name
for i in range((full_name_len-1)//10):
    full_name.middle_name_2[i] = first_name.first_name[random.randrange(first_name_len-1)]
    print(i)

# fill na with whitespace for people have no middle name 
full_name.middle_name_1 = full_name.middle_name_1.fillna('')
full_name.middle_name_2 = full_name.middle_name_2.fillna('')

# concate names into full name
#full_name['full_name'] = full_name['first_name'].map(str) + ' ' + full_name['middle_name_1'].map(str) + ' ' + full_name['middle_name_2'].map(str)+ ' ' + full_name['last_name'].map(str)

#remove extra whitespace
#full_name['full_name']=full_name['full_name'].replace({' +':' '},regex=True)

#full_name['full_name'] = full_name['full_name'].apply(lambda x: string.capwords(x))

output_name = full_name.full_name

# save to csv
output_name.write.csv('output.csv')