import pandas as pd
from sensor.config import mongo_client
from sensor.logger import logging
from sensor.exception import SensorException
import os,sys
import yaml
import dill
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
        print(df.shape)
        logging.info(f"Found Columns : {df.columns}")
        if "_id" in df.columns:
            logging.info(f"Dropping columns :_id")
            df.drop("_id",axis=1,inplace=True)
        logging.info(f"Row and columns : {df.shape[0]} , {df.shape[1]}")
        return df

    except Exception as e:
        raise SensorException(e, sys)

def write_yaml_file(file_path,data:dict):
    try:
        file_dir = os.path.dirname(file_path)
        os.makedirs(file_dir,exist_ok=True)
        with open(file_path,"w") as file_writer:
            yaml.dump(data,file_writer)

    except Exception as e:
        raise SensorException(e, sys)

def convert_columns_float(df,exclude_columns:list):
    for column in df.columns:
        if column not in exclude_columns:
            df[column] = df[column].astype('float')
    return df

def save_object(file_path:str, obj:object)->None:
    try:
        logging.info("Entered the save_object method of MainUtils class")
        os.makedirs(os.path.dirname(file_path),exist_ok=True)
        with open(file_path,"wb") as file_obj:
            dill.dump(obj,file_obj)
        logging.info("Exited the save_object method of MainUtils class")
    except Exception as e:
        raise SensorException(e, sys)

def load_object(file_path:str)->object:
    try:
        if not os.path.exists(file_path):
            raise Exception(f"The file: {file_path} is not exists")
        with open(file_path,"rb") as file_obj:
            return dill.load(file_obj)
    except Exception as e:
        raise SensorException(e, sys)

def save_numpy_array_data(file_path:str, array: np.array):
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
        with open(file_path,"wb") as file_obj:
            np.save(file_obj,array)
    except Exception as e:
        raise SensorException(e, sys)


def load_numpy_array_data(file_path: str) -> np.array:
    """
    load numpy array data from file
    file_path: str location of file to load
    return: np.array data loaded
    """
    try:
        with open(file_path, "rb") as file_obj:
            return np.load(file_obj)
    except Exception as e:
        raise SensorException(e, sys)