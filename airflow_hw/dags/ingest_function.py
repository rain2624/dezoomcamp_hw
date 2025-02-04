import os
import pandas as pd
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine
from time import time

# ## This script will store parquet file to database
def ingest_callable(user, password, host, port, db, table_name, parquet_file, execution_date):
    print(table_name, parquet_file, execution_date)    
    
    # Creating connection with postgresql
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print(engine.connect())
    print("Connection established successfully, Start inserting data.")

    file = pq.ParquetFile(parquet_file)
    df = next(file.iter_batches(batch_size=10)).to_pandas()
    df_iter = file.iter_batches(batch_size=10000)
    
    try:
        df.head(0).to_sql(name=table_name, con=engine.connect(), if_exists='replace')
        print("Table created")

    except Exception as e:
        print(e)

    t_start = time()

    count =0
    for batch in df_iter:
        count += 1

        batch_df = batch.to_pandas()

        print(f'inserting batch {count}...')
        try:
            b_start=time()
            batch_df.to_sql(table_name, con=engine.connect(), if_exists='append')
            b_end=time()

            print(f"Inserted batch {count} in {b_end-b_start:10.3f} seconds. \n")

        except Exception as e:
            print(e)

    t_end = time()   
    print(f'Completed! Total time taken was {t_end-t_start:10.3f} seconds for {count} batches.')    




    