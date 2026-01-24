#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd

# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz', nrows=100)

# Display first rows
df.head()

# Check data types
df.dtypes

# Check data shape
df.shape


# In[2]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)


# In[3]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[4]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[5]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[6]:


df.head(5)


# In[8]:


df_iter = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)


# In[9]:


from tqdm.auto import tqdm

first = True

with engine.begin() as conn:  

    for df_chunk in tqdm(df_iter):

        print("Chunk shape:", df_chunk.shape)

        if first:
            df_chunk.head(0).to_sql(
                name="yellow_taxi_data",
                con=conn,
                if_exists="replace",
                index=False
            )
            first = False
            print("Table created")

        df_chunk.to_sql(
            name="yellow_taxi_data",
            con=conn,
            if_exists="append",
            index=False
        )

        print("Inserted:", len(df_chunk))


# In[ ]:




