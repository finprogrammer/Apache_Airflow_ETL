import os
import sys
import pandas as pd
from scipy.stats import ks_2samp

from networksecurity.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file


class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            expected_num_columns = len(self._schema_config["columns"])
            logging.info(f"Expected columns: {expected_num_columns}, Actual columns: {len(dataframe.columns)}")
            return len(dataframe.columns) == expected_num_columns
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def detect_dataset_drift(self, base_df: pd.DataFrame, current_df: pd.DataFrame, threshold: float = 0.05) -> bool:
        try:
            status = True
            report = {}

            for column in base_df.columns:
                base_data = base_df[column]
                current_data = current_df[column]

                result = ks_2samp(base_data, current_data)
                drift_detected = result.pvalue < threshold

                if drift_detected:
                    status = False

                report[column] = {
                    "p_value": float(result.pvalue),
                    "drift_status": drift_detected,
                }

            os.makedirs(os.path.dirname(self.data_validation_config.drift_report_file_path), exist_ok=True)
            write_yaml_file(self.data_validation_config.drift_report_file_path, content=report)
            return status
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            train_df = self.read_data(train_file_path)
            test_df = self.read_data(test_file_path)

            # Validate column count
            if not self.validate_number_of_columns(train_df):
                raise NetworkSecurityException("Train dataset column count mismatch.", sys)
            if not self.validate_number_of_columns(test_df):
                raise NetworkSecurityException("Test dataset column count mismatch.", sys)

            # Perform data drift check
            drift_status = self.detect_dataset_drift(train_df, test_df)

            # Save validated data
            os.makedirs(os.path.dirname(self.data_validation_config.valid_train_file_path), exist_ok=True)
            train_df.to_csv(self.data_validation_config.valid_train_file_path, index=False)
            test_df.to_csv(self.data_validation_config.valid_test_file_path, index=False)

            # Build and return artifact
            return DataValidationArtifact(
                validation_status=drift_status,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )

        except Exception as e:
            raise NetworkSecurityException(e, sys)
