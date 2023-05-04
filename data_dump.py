import pymongo
import pandas as pd
import json

# Provide the mongodb localhost url to connect python to mongodb.
client = pymongo.MongoClient("mongodb://localhost:27017/neurolabDB")

DATA_FILE_PATH = '/config/workspace/aps_failure_training_set1.csv'
DATABASE_NAME = "aps"
COLLECTION_NAME = 'sensor'

if __name__=='__main__':
    df = pd.read_csv('/config/workspace/aps_failure_training_set1.csv')
    print(f'rows and column : {df.shape}')

    # convert dataframe to json so that we can dump this record in mongo db

    df.reset_index(drop=True,inplace=True)
    json_records = list(json.loads(df.T.to_json()).values())
    print(json_records[0])
    # inserted json resocrd to mongo db
    
    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_records)
