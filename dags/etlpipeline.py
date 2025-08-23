

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "evolonics",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="ml_data_pipeline",
    description="Data ingestion → validation → transformation",
    schedule="@weekly",                 # Airflow 2+: use `schedule`
    start_date=datetime(2024, 1, 1),
    catchup=False,                      # put catchup on DAG, not in default_args
    default_args=default_args,
    tags=["ml", "pipeline", "airflow"],
) as dag:

    def run_data_ingestion(**kwargs) -> dict:

        from networksecurity.entity.config_entity import TrainingPipelineConfig, DataIngestionConfig
        from networksecurity.components.data_ingestion import DataIngestion

        training_config = TrainingPipelineConfig()
        di_config = DataIngestionConfig(training_config)
        ingestion = DataIngestion(di_config)

        art = ingestion.initiate_data_ingestion()

        return {
            "train_csv": art.trained_file_path,
            "test_csv": art.test_file_path,
        }

    def run_data_validation(ti, **kwargs) -> dict:
        from networksecurity.entity.config_entity import TrainingPipelineConfig, DataValidationConfig
        from networksecurity.entity.artifact_entity import DataIngestionArtifact
        from networksecurity.components.data_validation import DataValidation

        ingest = ti.xcom_pull(task_ids="data_ingestion")
        di_art = DataIngestionArtifact(
            trained_file_path=ingest["train_csv"],
            test_file_path=ingest["test_csv"],
        )

        training_config = TrainingPipelineConfig()
        dv_config = DataValidationConfig(training_config)
        validator = DataValidation(di_art, dv_config)

        dv_art = validator.initiate_data_validation()

        return {
            "valid_train_csv": dv_art.valid_train_file_path,
            "valid_test_csv": dv_art.valid_test_file_path,
            "drift_report": dv_art.drift_report_file_path,
            "validation_status": bool(dv_art.validation_status),
        }

    def run_data_transformation(ti, **kwargs) -> dict:
        from networksecurity.entity.config_entity import TrainingPipelineConfig, DataTransformationConfig
        from networksecurity.entity.artifact_entity import DataValidationArtifact
        from networksecurity.components.data_transformation import DataTransformation

        val = ti.xcom_pull(task_ids="data_validation")

        dv_art = DataValidationArtifact(
            validation_status=bool(val.get("validation_status", True)),
            valid_train_file_path=val["valid_train_csv"],
            valid_test_file_path=val["valid_test_csv"],
            invalid_train_file_path=None,
            invalid_test_file_path=None,
            drift_report_file_path=val["drift_report"],
        )

        training_config = TrainingPipelineConfig()
        dt_config = DataTransformationConfig(training_config)
        transformer = DataTransformation(dv_art, dt_config)

        dt_art = transformer.initiate_data_transformation()

        return {
            "transformed_train": dt_art.transformed_train_file_path,
            "transformed_test": dt_art.transformed_test_file_path,
            "preprocessor_pkl": dt_art.transformed_object_file_path,
        }

    task_data_ingestion = PythonOperator(
        task_id="data_ingestion",
        python_callable=run_data_ingestion,
    )

    task_data_validation = PythonOperator(
        task_id="data_validation",
        python_callable=run_data_validation,
    )

    task_data_transformation = PythonOperator(
        task_id="data_transformation",
        python_callable=run_data_transformation,
    )

    task_data_ingestion >> task_data_validation >> task_data_transformation
