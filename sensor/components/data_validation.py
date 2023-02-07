
from sensor.exception import SensorException
from sensor.logger import logging
from sensor.entity import artifact_entity, config_entity
from scipy.stats import ks_2samp
import os, sys
import pandas as pd
from typing import Optional
from sensor import utils
import numpy as np

class DataValidation:
    def __init__(self, 
                data_validation_config:config_entity.DataValidationConfig,
                data_ingestion_artifact: artifact_entity.DataIngestionArtifact):

        try:
            logging.info(f"{'>>'*20} Data Validation {'<<'*20}")
            self.data_validation_config = data_validation_config
            self.validation_error = dict()
            self.data_ingestion_artifact = data_ingestion_artifact

        except Exception as e:
            raise SensorException(e, sys)    
    


    def drop_missing_value_columns(self, df:pd.DataFrame,report_key_name:str) -> pd.DataFrame:
        """
        This function drops column that contains missing value more than specified threshold
        df: Accepts a pandas dataframe
        threshold: Percentage criteria to drop a column
        ================================================================================================
        return Pandas DataFrame if atleaset single column is available after missing column else None
        """
        
        try:
            logging.info('electing the columns name with null value more than threshold')
            threshold = self.data_validation_config.missing_threshold
            null_report = df.isna().sum()/df.shape[0]
            # selecting the columns name with null value more than threshold
            drop_columns_name = null_report[null_report > threshold].index
            
            logging.info(f'Dropped columns {list(drop_columns_name)}')

            self.validation_error[report_key_name] = list(drop_columns_name)
            df.drop(list(drop_columns_name), axis = 1, inplace = True)

            # Return none is no columns are left.         
            if len(df.columns) == 0:
            
                return None
            return df
        except Exception as e:
            raise SensorException(e, sys) 
        
    def is_required_column_exists(  self,
                                    base_df:pd.DataFrame, 
                                    curr_df:pd.DataFrame,
                                    report_key_name:str ) -> bool:

        try:
            base_columns = base_df.columns
            current_columns = curr_df.columns
            missing_columns = []
            for base_column in base_columns:
                if base_column not in current_columns:
                    logging.info(f'columns not present: {base_column}')
                    missing_columns.append(base_column)

            if len(missing_columns) > 0:
                self.validation_error[report_key_name] = missing_columns
                return False
            return True

        except Exception as e:
            raise SensorException(e, sys)

    def data_drift(self,base_df: pd.DataFrame, curr_df: pd.DataFrame, report_key_name:str):
        try:
            drift_report = dict()
            base_columns = base_df.columns
            current_columns = curr_df.columns
            for base_column in base_columns:
                base_data,current_data = base_df[base_column], curr_df[base_column]
                same_distribution = ks_2samp(base_data, current_data)
                
                if same_distribution.pvalue > .05:
                    drift_report[base_column] = {
                        'pvalue': float(same_distribution.pvalue),
                        'same_distribution' : True
                    }
                    #same disrtibution 
                else:
                    # different distribution 
                    drift_report[base_column] = {
                        'pvalue': float(same_distribution.pvalue),
                        'same_distribution' : False
                    }
                    pass

            self.validation_error[report_key_name] = drift_report
            
        except Exception as e:
            raise SensorException(e, sys)



    def initiate_data_validation(self) -> artifact_entity.DataValidationArtifact:
        try:
            base_df = pd.read_csv(self.data_validation_config.base_file_path)
            # base Data Frame has na as null values 
            base_df.replace({'na': np.NAN}, inplace=True)
            base_df = self.drop_missing_value_columns(df = base_df, report_key_name = 'missing_values_within_base_datsset')


            train_df = pd.read_csv(self.data_ingestion_artifact.train_file_path)
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)

            train_df = self.drop_missing_value_columns( df = train_df, report_key_name = 'missing_values_within_train_datsset')
            test_df = self.drop_missing_value_columns(df = test_df,report_key_name = 'missing_values_within_test_datsset')

            exclude_col = ['class']
            base_df = utils.convert_columns_float(df = base_df, exclude_columns= exclude_col )
            train_df = utils.convert_columns_float(df = train_df, exclude_columns = exclude_col)
            test_df = utils.convert_columns_float(df = test_df, exclude_columns=  exclude_col)

            train_df_columns_status = self.is_required_column_exists(base_df = base_df, curr_df = train_df,report_key_name = 'missing_columns_within_train_datsset')
            test_df_columns_status = self.is_required_column_exists(base_df = base_df, curr_df = test_df, report_key_name = 'missing_columns_within_test_datsset')

            if train_df_columns_status:
                self.data_drift(base_df=base_df, curr_df=train_df,report_key_name = 'data_drift_within_train_datsset')
            if test_df_columns_status:
                self.data_drift(base_df = base_df, curr_df = test_df, report_key_name = 'data_drift_within_test_datsset')
            
            # write the report
            utils.write_yaml_file(  file_path = self.data_validation_config.report_file_path, data = self.validation_error)
            data_validation_artifact = artifact_entity.DataValidationArtifact(report_file_path = self.data_validation_config.report_file_path)    
            
            return data_validation_artifact
        except Exception as e:
            raise SensorException(e, sys)
