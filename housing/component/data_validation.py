from housing.logger import logging
from housing.exception import HousingException
from housing.entity.config_entity import DataValidationConfig
from housing.entity.artifact_entity import DataIngestionArtifact



class DataValidation:
    
    def __init__(self, data_validatioin_config:DataValidationConfig,
        data_ingestion_artifact:DataIngestionArtifact):
        try:
            self.data_validatioin_config = data_validatioin_config
            self.data_ingestion_artifact = data_ingestion_artifact
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



    

    def initiate_data_validation(self):
        try:
            self.is_train_test_file_exists()
            self.validate_dataset_schema()

        except Exception as e:
            raise HousingException(e,sys) from e
