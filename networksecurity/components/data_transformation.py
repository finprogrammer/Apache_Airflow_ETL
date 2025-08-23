import sys
import os
import numpy as np
import pandas as pd

from sklearn.impute import SimpleImputer, KNNImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler, PowerTransformer, LabelEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.base import BaseEstimator, TransformerMixin

from networksecurity.constant.training_pipeline import TARGET_COLUMN, DATA_TRANSFORMATION_IMPUTER_PARAMS
from networksecurity.entity.artifact_entity import DataTransformationArtifact, DataValidationArtifact
from networksecurity.entity.config_entity import DataTransformationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.utils.main_utils.utils import save_numpy_array_data, save_object


class DataTransformation:
    def __init__(self, data_validation_artifact: DataValidationArtifact, data_transformation_config: DataTransformationConfig):
        try:
            self.val = data_validation_artifact
            self.cfg = data_transformation_config
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    @staticmethod
    def _read_csv(path: str) -> pd.DataFrame:
        try:
            return pd.read_csv(path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    @staticmethod
    def _is_binary_or_numeric_target(y: pd.Series) -> bool:
        if pd.api.types.is_numeric_dtype(y):
            return True
        return y.nunique() <= 2

    def _build_transformer(self, X: pd.DataFrame) -> ColumnTransformer:
        try:
            obj_cols = X.select_dtypes(include=["object", "category"]).columns.tolist()
            num_cols = X.select_dtypes(include=["number", "bool"]).columns.tolist()

            skew_threshold = 1.0
            skewed = [c for c in num_cols if np.isfinite(X[c]).any() and abs(pd.Series(X[c]).dropna().skew()) > skew_threshold]
            regular = [c for c in num_cols if c not in skewed]

            numeric_knn = Pipeline([
                ("impute_knn", KNNImputer(**DATA_TRANSFORMATION_IMPUTER_PARAMS)),
                ("scale", StandardScaler())
            ])

            numeric_normal = Pipeline([
                ("impute_mean", SimpleImputer(strategy="mean")),
                ("scale", StandardScaler())
            ])

            numeric_skewed = Pipeline([
                ("impute_mean", SimpleImputer(strategy="mean")),
                ("yeojohnson", PowerTransformer(method="yeo-johnson")),
                ("scale", StandardScaler())
            ])

            categorical = Pipeline([
                ("impute_mode", SimpleImputer(strategy="most_frequent")),
                ("onehot", OneHotEncoder(handle_unknown="ignore", sparse=True))
            ])

            transformers = []
            # Prefer KNN for general numeric when feasible; still use specialized pipes for skewness
            if skewed:
                transformers.append(("num_skewed", numeric_skewed, skewed))
            if regular:
                # If KNN is desired globally, use it for the remaining numerics
                transformers.append(("num_regular", numeric_knn, regular))
            if obj_cols:
                transformers.append(("cat", categorical, obj_cols))

            if not transformers:
                raise NetworkSecurityException("No valid feature columns found for transformation.", sys)

            return ColumnTransformer(transformers, remainder="drop", sparse_threshold=1.0)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def run(self) -> DataTransformationArtifact:
        logging.info("Starting data transformation")
        try:
            train_df = self._read_csv(self.val.valid_train_file_path)
            test_df = self._read_csv(self.val.valid_test_file_path)

            if TARGET_COLUMN not in train_df.columns or TARGET_COLUMN not in test_df.columns:
                raise NetworkSecurityException(f"Missing target column '{TARGET_COLUMN}' in input files.", sys)

            y_train = train_df[TARGET_COLUMN]
            y_test = test_df[TARGET_COLUMN]

            # Normalize common binary encodings
            if pd.api.types.is_numeric_dtype(y_train):
                y_train = y_train.replace(-1, 0)
                y_test = y_test.replace(-1, 0)

            X_train = train_df.drop(columns=[TARGET_COLUMN])
            X_test = test_df.drop(columns=[TARGET_COLUMN])

            preprocessor = self._build_transformer(X_train)

            y_encoder = None
            if not self._is_binary_or_numeric_target(y_train):
                y_encoder = LabelEncoder()
                y_train = y_encoder.fit_transform(y_train)
                y_test = y_encoder.transform(y_test)
                save_object(os.path.join("final_model", "label_encoder.pkl"), y_encoder)
            else:
                # Ensure numeric dtype for downstream models
                if not pd.api.types.is_numeric_dtype(y_train):
                    y_encoder = LabelEncoder()
                    y_train = y_encoder.fit_transform(y_train)
                    y_test = y_encoder.transform(y_test)
                    save_object(os.path.join("final_model", "label_encoder.pkl"), y_encoder)

            preprocessor_fitted = preprocessor.fit(X_train)
            X_train_t = preprocessor_fitted.transform(X_train)
            X_test_t = preprocessor_fitted.transform(X_test)

            if hasattr(X_train_t, "toarray"):
                X_train_t = X_train_t.toarray()
            if hasattr(X_test_t, "toarray"):
                X_test_t = X_test_t.toarray()

            train_arr = np.c_[X_train_t, np.asarray(y_train).reshape(-1, 1)]
            test_arr = np.c_[X_test_t, np.asarray(y_test).reshape(-1, 1)]

            save_numpy_array_data(self.cfg.transformed_train_file_path, train_arr)
            save_numpy_array_data(self.cfg.transformed_test_file_path, test_arr)
            save_object(self.cfg.transformed_object_file_path, preprocessor_fitted)
            save_object(os.path.join("final_model", "preprocessor.pkl"), preprocessor_fitted)

            return DataTransformationArtifact(
                transformed_object_file_path=self.cfg.transformed_object_file_path,
                transformed_train_file_path=self.cfg.transformed_train_file_path,
                transformed_test_file_path=self.cfg.transformed_test_file_path,
            )
        except Exception as e:
            raise NetworkSecurityException(e, sys)
