from sensor.entity import artifact_entity,config_entity
from sensor.exception import SensorException
from sensor.logger import logging
from scipy.stats import ks_2samp
import os, sys 
import pandas as pd
from typing import Optional
from sensor import utils
import numpy as np
from sensor.config import TARGET_COLUMN


class DataValidation:
    def __init__(self,data_validation_config:config_entity.DataValidationConfig,data_ingestion_artifact:artifact_entity.DataIngestionArtifact):
        try:
            logging.info
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact= data_ingestion_artifact
            self.validation_error = dict()
        except Exception as e:
            raise SensorException(e, sys)


    def drop_missing_values_columns(self, df:pd.DataFrame, report_key_name:str):
        """
        this function will drop column which contains missing value more than specified threshold
        df: Accepts a pandas dataframe
        threshold: percentage criteria to drop a column
        =========================================================================
        returns Pandas Dataframe if atleast a single column is available after missing columns drop else NOne 
        """
        try:
            self.threshold = self.data_validation_config.missing_threshold
            drop_columns_names =[]
            null_report = df.isna().sum()/df.shape[0]
            #selecting column names which contain the null values
            drop_column_names = null_report[null_report>0.3].index
            self.validation_error[report_key_name]=list(drop_column_names)
            df.drop(list(drop_column_names),axis =1,inplace = True)
            if len(df.columns)==0:
                return None
            return df
        except Exception as e:
            raise SensorException(e,sys)
        
    def is_required_columns_exists(self,base_df:pd.DataFrame,current_df:pd.DataFrame,report_key_name:str)->bool:
        try:
            missing_columns=[]   
            base_columns = base_df.columns
            current_columns = current_df.columns
            for base_column in base_columns:
                if base_column not in current_columns:
                    missing_columns.append(base_column)
            if len(missing_columns)>0:
                self.validation_error[report_key_name]=missing_columns
                return False
            else:
                return True
        except Exception as e:
            raise SensorException(e, sys)
        
    def data_drift(self,base_df:pd.DataFrame,current_df:pd.DataFrame,report_key_name:str):
        try:
            drift_report=dict()
            base_columns = base_df.columns
            current_columns = current_df.columns
            for base_column in base_columns:
                base_data,current_data = base_df[base_column], current_df[base_column]
                #null hypothesis is that both column data drwan from same distribution
                same_distribution = ks_2samp(base_data,current_data)
                print(same_distribution)
                if same_distribution.pvalue>0.05:
                    drift_report[base_column]={"pvalues":float(same_distribution.pvalue),
                                                    "same_distribution":True}
                        #same distribution
                        
                else:
                    drift_report[base_column]={"pvalues":float(same_distribution.pvalue),
                                                    "same_distribution":False}
                                                
                        #different distribution
            self.validation_error[report_key_name]=drift_report


        except Exception as e:
            raise SensorException(e, sys)
                



    def initiate_data_validation(self)->artifact_entity.DataValidationArtifact:
        try:
            base_df = pd.read_csv(self.data_validation_config.base_file_path)
            base_df.replace({"na":np.NAN},inplace=True)
            base_df = self.drop_missing_values_columns(df=base_df,report_key_name="missing_values_base_dataset")
            train_df = pd.read_csv(self.data_ingestion_artifact.train_file_path)
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)

            train_df = self.drop_missing_values_columns(df=train_df,report_key_name="missing_values_train_dataset")
            test_df=self.drop_missing_values_columns(df=test_df,report_key_name="missing_values_test_dataset")

            exclude_columns= [TARGET_COLUMN]
            base_df = utils.convert_columns_float(df=base_df, exclude_columns=exclude_columns)
            train_df = utils.convert_columns_float(df=train_df, exclude_columns=exclude_columns)
            test_df = utils.convert_columns_float(df=test_df, exclude_columns=exclude_columns)

            train_df_columns_states = self.is_required_columns_exists(base_df=base_df,current_df=train_df,report_key_name="missing_columns_train_df")
            test_df_columns_states = self.is_required_columns_exists(base_df=base_df,current_df=test_df,report_key_name="missing_columns_test_df")

            if train_df_columns_states:
                self.data_drift(base_df=base_df,current_df=train_df,report_key_name="data_drift_train_df")
            if test_df_columns_states:
                self.data_drift(base_df=base_df,current_df=test_df,report_key_name="data_drift_test_df")

            utils.write_yaml_file(file_path=self.data_validation_config.report_file_path, data=self.validation_error)
            data_validation_artifact = artifact_entity.DataValidationArtifact(report_file_path=self.data_validation_config.report_file_path)
            return data_validation_artifact


        except Exception as e:
            raise SensorException(e, sys)