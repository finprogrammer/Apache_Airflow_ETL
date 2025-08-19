import sys
import os
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler, LabelEncoder, PowerTransformer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.base import BaseEstimator, TransformerMixin
from networksecurity.constant.training_pipeline import TARGET_COLUMN
from networksecurity.entity.artifact_entity import DataTransformationArtifact, DataValidationArtifact
from networksecurity.entity.config_entity import DataTransformationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.utils.main_utils.utils import save_numpy_array_data, save_object
from sklearn.impute import KNNImputer
from networksecurity.constant.training_pipeline import (
    DATA_TRANSFORMATION_IMPUTER_PARAMS,
)


class DataTransformation:
    def __init__(
        self,
        data_validation_artifact: DataValidationArtifact,
        data_transformation_config: DataTransformationConfig,
    ):
        try:
            self.data_validation_artifact: DataValidationArtifact = (
                data_validation_artifact
            )
            self.data_transformation_config: DataTransformationConfig = (
                data_transformation_config
            )
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def get_data_transformer_object(cls) -> Pipeline:
        """
        It initialises a KNNImputer object with the parameters specified in the training_pipeline.py file
        and returns a Pipeline object with the KNNImputer object as the first step.

        Args:
          cls: DataTransformation

        Returns:
          A Pipeline object
        """
        logging.info(
            "Entered get_data_trnasformer_object method of Trnasformation class"
        )
        try:
            imputer: KNNImputer = KNNImputer(
                **DATA_TRANSFORMATION_IMPUTER_PARAMS
            )  # that is imputer = KNNImputer(n_neighbors=3, weights="uniform")
            logging.info(
                f"Initialise KNNImputer with {DATA_TRANSFORMATION_IMPUTER_PARAMS}"
            )
            processor: Pipeline = Pipeline(
                [("imputer", imputer)]
            )  # Name: "imputer", Step: KNNImputer
            """This wraps the KNNImputer inside a Pipeline object from sklearn.pipeline.

            Pipeline allows you to chain together preprocessing steps (and optionally models) into a single object.

            This step sets up a 1-step pipeline"""
            return processor
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_transformation(self) -> DataTransformationArtifact:
        logging.info(
            "Entered initiate_data_transformation method of DataTransformation class"
        )
        try:
            logging.info("Starting data transformation")
            train_df = DataTransformation.read_data(
                self.data_validation_artifact.valid_train_file_path
            )
            test_df = DataTransformation.read_data(
                self.data_validation_artifact.valid_test_file_path
            )

            ## training dataframe
            input_feature_train_df = train_df.drop(columns=[TARGET_COLUMN], axis=1)
            target_feature_train_df = train_df[TARGET_COLUMN]
            target_feature_train_df = target_feature_train_df.replace(-1, 0)

            # testing dataframe
            input_feature_test_df = test_df.drop(columns=[TARGET_COLUMN], axis=1)
            target_feature_test_df = test_df[TARGET_COLUMN]
            target_feature_test_df = target_feature_test_df.replace(-1, 0)

            preprocessor = (
                self.get_data_transformer_object()
            )  # Calls the method get_data_transformer_object() from the class

            preprocessor_object = preprocessor.fit(input_feature_train_df)
            """fit(): This trains the imputer on the training data only (i.e., calculates which values to use for imputing missing data based on K-Nearest Neighbors).

                Returns a fitted pipeline object, stored in preprocessor_object"""
            transformed_input_train_feature = preprocessor_object.transform(
                input_feature_train_df
            )
            """transform(): This applies the learned imputation logic to the training data.

            Missing values in input_feature_train_df are filled using the strategy learned during fitting"""
            transformed_input_test_feature = preprocessor_object.transform(
                input_feature_test_df
            )
            """Applies the same transformation (learned from training data) to the test data.

            This ensures consistent preprocessing between train and test sets, which is critical to avoid data leakage."""

            """ You can't use fit_transform() on the test set — only transform() should be applied after fitting"""

            train_arr = np.c_[
                transformed_input_train_feature, np.array(target_feature_train_df)
            ]
            test_arr = np.c_[
                transformed_input_test_feature, np.array(target_feature_test_df)
            ]
            """np.c_ is a NumPy convenience function that concatenates arrays column-wise.

            It's like saying:
            combine these arrays side by side (i.e., as columns)."""

            # save numpy array data
            save_numpy_array_data(
                self.data_transformation_config.transformed_train_file_path,
                array=train_arr,
            )
            save_numpy_array_data(
                self.data_transformation_config.transformed_test_file_path,
                array=test_arr,
            )
            save_object(
                self.data_transformation_config.transformed_object_file_path,
                preprocessor_object,
            )

            save_object(
                "final_model/preprocessor.pkl",
                preprocessor_object,
            )  # for model pusher putting in one source from where it can be pushed to s3 bucket

            # preparing artifacts

            data_transformation_artifact = DataTransformationArtifact(
                transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
                transformed_train_file_path=self.data_transformation_config.transformed_train_file_path,
                transformed_test_file_path=self.data_transformation_config.transformed_test_file_path,
            )
            return data_transformation_artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)

class LogFeatureAdder(BaseEstimator, TransformerMixin):
    def __init__(self, features):
        self.features = features

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = X.copy()
        for feature in self.features:
            if feature in X.columns:
                X[f"{feature}_log"] = np.log1p(X[feature])
        return X


class DataPreprocessor:
    def __init__(self, validation_artifact: DataValidationArtifact, config: DataTransformationConfig):
        try:
            self.validation_artifact = validation_artifact
            self.config = config
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    @staticmethod
    def load_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def construct_transformer(self, df: pd.DataFrame) -> ColumnTransformer:
        try:
            logging.info("Creating transformation pipeline...")

            label_columns = [TARGET_COLUMN, 'label']
            numeric_features = df.select_dtypes(include=['int64', 'float64']).columns.difference(label_columns).tolist()
            categorical_features = df.select_dtypes(include=['object']).columns.difference(label_columns).tolist()

            skew_threshold = 1.0
            skewed_numeric = [col for col in numeric_features if abs(df[col].skew()) > skew_threshold]
            regular_numeric = list(set(numeric_features) - set(skewed_numeric))

            logging.info(f"Highly skewed numeric features: {skewed_numeric}")
            logging.info(f"Normal numeric features: {regular_numeric}")

            skewed_pipeline = Pipeline([
                ("imputer", SimpleImputer(strategy="mean")),
                ("power_transform", PowerTransformer(method="yeo-johnson")),
                ("scale", StandardScaler())
            ])

            normal_pipeline = Pipeline([
                ("imputer", SimpleImputer(strategy="mean")),
                ("scale", StandardScaler())
            ])

            categorical_pipeline = Pipeline([
                ("imputer", SimpleImputer(strategy="most_frequent")),
                ("encode", OneHotEncoder(handle_unknown="ignore"))
            ])

            full_transformer = ColumnTransformer([
                ("skewed_numeric", skewed_pipeline, skewed_numeric),
                ("normal_numeric", normal_pipeline, regular_numeric),
                # ("categorical", categorical_pipeline, categorical_features)  # Uncomment if needed
            ])

            return full_transformer

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def execute_transformation(self) -> DataTransformationArtifact:
        try:
            logging.info("Starting transformation on input datasets...")

            train_df = self.load_data(self.validation_artifact.valid_train_file_path)
            test_df = self.load_data(self.validation_artifact.valid_test_file_path)

            X_train = train_df.drop(columns=[TARGET_COLUMN])
            y_train = train_df[TARGET_COLUMN]

            X_test = test_df.drop(columns=[TARGET_COLUMN])
            y_test = test_df[TARGET_COLUMN]

            encoder = LabelEncoder()
            y_train_encoded = encoder.fit_transform(y_train)
            y_test_encoded = encoder.transform(y_test)

            save_object("final_model/label_encoder.pkl", encoder)

            transformer = self.construct_transformer(X_train)

            fitted_transformer = transformer.fit(X_train)
            X_train_processed = fitted_transformer.transform(X_train)
            X_test_processed = fitted_transformer.transform(X_test)

            if hasattr(X_train_processed, "toarray"):
                X_train_processed = X_train_processed.toarray()
            if hasattr(X_test_processed, "toarray"):
                X_test_processed = X_test_processed.toarray()

            train_data = np.c_[X_train_processed, y_train_encoded.reshape(-1, 1)]
            test_data = np.c_[X_test_processed, y_test_encoded.reshape(-1, 1)]

            save_numpy_array_data(self.config.transformed_train_file_path, train_data)
            save_numpy_array_data(self.config.transformed_test_file_path, test_data)
            save_object(self.config.transformed_object_file_path, fitted_transformer)
            save_object("final_model/preprocessor.pkl", fitted_transformer)

            return DataTransformationArtifact(
                transformed_object_file_path=self.config.transformed_object_file_path,
                transformed_train_file_path=self.config.transformed_train_file_path,
                transformed_test_file_path=self.config.transformed_test_file_path,
            )

        except Exception as e:
            raise NetworkSecurityException(e, sys)
