import pandas as pd
from sensor.config import mongo_client
from sensor.logger import logging
from sensor.exception import SensorException
import os,sys

def get_collection_as_dataframe(database_name:str,collection_name:str)->pd.DataFrame:
    """
    Description : This function return collection as dataframe
    database_name : database name
    collection_name : collection name

    =====================================================
    retrun Pandas dataframe of a collection
    
    """
    try:
        logging.info("reading data from database : {database_name} and collection: {collection_name}")
        df= pd.DataFrame(list(mongo_client[database_name][collection_name].find()))
        logging.info(f"Found Columns : {df.columns}")
        if "_id" in df.columns:
            logging.info(f"Dropping columns :_id")
            df.drop("_id",axis=1)
        logging.info(f"Row and columns : {df.shape[0]} , {df.shape[1]}")

    except Exception as e:
        raise SensorException(e, sys)
