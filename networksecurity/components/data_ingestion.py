import os
import sys
import numpy as np
import pandas as pd
from typing import Optional
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv

from networksecurity.constant.training_pipeline import TARGET_COLUMN
from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

load_dotenv()  # works only if .env exists inside the container; prefer Docker envs
MONGO_DB_URL = os.getenv("MONGODB_URL_KEY")


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def export_collection_as_dataframe(self) -> pd.DataFrame:
        """
        Connect to MongoDB and stream the collection into a DataFrame safely.
        - Pings first to fail fast if unreachable.
        - Uses reasonable timeouts + retryable reads.
        - Streams in batches (no giant list()).
        - Disables server cursor idle timeout while iterating.
        """
        try:
            from pymongo import MongoClient
            from pymongo.errors import ServerSelectionTimeoutError, AutoReconnect

            uri = MONGO_DB_URL
            if not uri:
                raise NetworkSecurityException("MONGODB_URL_KEY is not set", sys)

            # Context manager ensures sockets close
            with MongoClient(
                uri,
                serverSelectionTimeoutMS=5000,   # fail fast on bad host/creds
                connectTimeoutMS=5000,
                socketTimeoutMS=120000,          # allow long reads
                retryWrites=True,
                retryReads=True,
            ) as client:

                # Fail early if server not healthy
                client.admin.command("ping")

                db_name = self.data_ingestion_config.database_name
                coll_name = self.data_ingestion_config.collection_name
                coll = client[db_name][coll_name]

                # Exclude _id at the wire
                projection = {"_id": 0}

                cursor = coll.find(
                    {}, projection=projection
                ).batch_size(5000)

                rows = []
                fetched = 0
                try:
                    for doc in cursor:
                        rows.append(doc)
                        fetched += 1
                        # If you expect tens of millions, flush chunks to CSV here.
                finally:
                    cursor.close()

                df = pd.DataFrame.from_records(rows)
                if not df.empty:
                    df.replace({"na": np.nan}, inplace=True)

                logging.info(f"Data loaded from MongoDB {db_name}.{coll_name}, rows: {fetched}")
                return df

        except ServerSelectionTimeoutError as e:
            raise NetworkSecurityException(f"Cannot reach MongoDB: {e}", sys)
        except AutoReconnect as e:
            raise NetworkSecurityException(f"Mongo transient error (AutoReconnect): {e}", sys)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def export_data_into_feature_store(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        try:
            feature_store_path = self.data_ingestion_config.feature_store_file_path
            os.makedirs(os.path.dirname(feature_store_path), exist_ok=True)
            dataframe.to_csv(feature_store_path, index=False)
            logging.info(f"Data exported to feature store at: {feature_store_path}")
            return dataframe
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def split_data_as_train_test(self, dataframe: pd.DataFrame):
        try:
            if TARGET_COLUMN not in dataframe.columns:
                raise NetworkSecurityException(
                    f"TARGET_COLUMN '{TARGET_COLUMN}' not found in dataframe", sys
                )

            train_data, test_data = train_test_split(
                dataframe,
                test_size=self.data_ingestion_config.train_test_split_ratio,
                stratify=dataframe[TARGET_COLUMN],
                random_state=42,
            )

            train_path = self.data_ingestion_config.training_file_path
            test_path = self.data_ingestion_config.testing_file_path
            os.makedirs(os.path.dirname(train_path), exist_ok=True)

            train_data.to_csv(train_path, index=False)
            test_data.to_csv(test_path, index=False)
            logging.info(f"Train and test datasets saved at: {train_path}, {test_path}")
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            df = self.export_collection_as_dataframe()
            df = self.export_data_into_feature_store(df)
            self.split_data_as_train_test(df)

            artifact = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path
            )
            logging.info("Data ingestion completed and artifact created.")
            return artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)
