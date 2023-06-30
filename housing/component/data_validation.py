from housing.logger import logging
from housing.exception import HousingException
from housing.entity.config_entity import DataValidationConfig
from housing.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
import os,sys
import pandas as pd
import json
from evidently.model_profile import Profile
from evidently.dashboard import Dashboard
from evidently.dashboard.tabs import DataDriftTab

from evidently.model_profile.sections import DataDriftProfileSection


class DataValidation:
    
    def __init__(self, data_validatioin_config:DataValidationConfig,
        data_ingestion_artifact:DataIngestionArtifact):
        try:
            self.data_validatioin_config = data_validatioin_config
            self.data_ingestion_artifact = data_ingestion_artifact
        except Exception as e:
            raise HousingException(e,sys) from e

    def get_train_and_test_df():
        try:
            train_df = pd.read_csv(self.data_ingestion_artifact.train_file_path)
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)
            return train_df,test_df
        except Exception as e:
            raise HousingException(e,sys) from e

    
    def is_train_test_file_exists(self):
        try:
            logging.info("Checking if training and test files exist")
            is_train_file_exist = False
            is_test_file_exist = False

            train_file_path  = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            is_train_file_exist = os.path.exists(train_file_path)
            is_test_file_exist = os.path.exists(test_file_path)



            is_available =  is_train_file_exist and is_test_file_exist

            logging.info(f"Is train and test files exists? ->{is_available}")
            if not is_available:
                training_file = self.data_ingestion_artifact.train_file_path
                testing_file = self.data_ingestion_artifact.test_file_path
                message = f"Training File: {training_file} or testing File: {testing_file} is not present"
                logging.info(message)
                raise Exception(message)
            
            
            return is_available
        except Exception as e:
            raise HousingException(e,sys) from e

    
    def validate_dataset_schema(self)->bool:
        try:
            validation_status = False



            validation_status = True
            return validation_status
        except Exception as e:
            raise HousingException(e,sys) from e

    def get_and_save_data_drift_report(self):
        try:
            profile = Profile(sections=[DataDriftProfileSection()])

            train_df,test_df = self.get_train_and_test_df()

            profile.calculate(train_df,test_df)

            report = json.loads(profile.json())

            report_file_path = self.data_validatioin_config.report_file_path

            report_dir = os.path.dirname(report_file_path)

            os.makedirs(report_dir,exist_ok = True)

            with open(report_file_path,"w") as report_file:
                json.dump(report,report_file,indent=6)

            return report





        except Exception as e:
            raise HousingException(e,sys) from e

    def save_data_drift_report_page(self):
        try:
            dashboard = Dashboard(tabs=[DataDriftTab()])
            train_df,test_df = self.get_train_and_test_df()
            dashboard.calculate(train_df,test_df)
            report_page_file_path = self.data_validatioin_config.report_page_file_path

            report_page_dir = os.path.dirname(report_page_file_path)

            os.makedirs(report_page_dir,exist_ok = True)





            dashboard.save(self.data_validatioin_config.report_page_file_path)
        except Exception as e:
            raise HousingException(e,sys) from e



    def is_data_drift_found(self)->bool:
        try:
            report = self.get_and_save_data_drift_report()
            self.save_data_drift_report_page()
            return True
            
        except Exception as e:
            raise HousingException(e,sys) from e




    

    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            self.is_train_test_file_exists()
            self.validate_dataset_schema()
            self.is_data_drift_found()

            data_validation_artifact = DataValidationArtifact(
                schema_file_path = self.data_validatioin_config.schema_file_path,
                report_file_path =  self.data_validatioin_config.report_file_path,
                report_page_file_path = self.data_validatioin_config.report_page_file_path,
                is_validated = True,
                message = "Data Validation performed succesfully")

            logging.info("Data Validation Artifact :{data_validation_artifact}")

        except Exception as e:
            raise HousingException(e,sys) from e
