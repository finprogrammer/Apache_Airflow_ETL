import os
import sys
import yaml
import pickle
import numpy as np
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import f1_score

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging


def read_yaml_file(file_path: str) -> dict:
    """
    Load configuration from a YAML file.
    """
    try:
        with open(file_path, "r") as yaml_stream:
            return yaml.safe_load(yaml_stream)
    except Exception as e:
        raise NetworkSecurityException(e, sys)


def write_yaml_file(file_path: str, content: object, replace: bool = False) -> None:
    """
    Write data to a YAML file, replacing the file if specified.
    """
    try:
        if replace and os.path.exists(file_path):
            os.remove(file_path)

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w") as yaml_file:
            yaml.dump(content, yaml_file)

    except Exception as e:
        raise NetworkSecurityException(e, sys)


def save_numpy_array_data(file_path: str, array: np.array) -> None:
    """
    Store a numpy array to a file in binary format.
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file:
            np.save(file, array)
    except Exception as e:
        raise NetworkSecurityException(e, sys)


def load_numpy_array_data(file_path: str) -> np.array:
    """
    Load numpy array from the specified file path.
    """
    try:
        with open(file_path, "rb") as file:
            return np.load(file, allow_pickle=True)
    except Exception as e:
        raise NetworkSecurityException(e, sys)


def save_object(file_path: str, obj: object) -> None:
    """
    Serialize and save an object using pickle.
    """
    try:
        logging.info("Saving object to file...")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "wb") as file:
            pickle.dump(obj, file)

        logging.info("Object successfully saved.")

    except Exception as e:
        raise NetworkSecurityException(e, sys)


def load_object(file_path: str) -> object:
    """
    Load and return a pickled object from file.
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, "rb") as file:
            return pickle.load(file)

    except Exception as e:
        raise NetworkSecurityException(e, sys)


def evaluate_models(X_train, y_train, X_test, y_test, models, param) -> dict:
    """
    Perform hyperparameter tuning and evaluate each model using weighted F1 score.
    Returns a dictionary with model names and their corresponding test F1 scores.
    """
    try:
        performance_report = {}

        for model_name, model_instance in models.items():
            model_params = param.get(model_name, {})

            # Perform grid search
            grid_search = GridSearchCV(model_instance, model_params, cv=3)
            grid_search.fit(X_train, y_train)

            # Set best parameters and retrain
            best_model = grid_search.best_estimator_
            best_model.fit(X_train, y_train)

            # Predict and evaluate
            y_train_pred = best_model.predict(X_train)
            y_test_pred = best_model.predict(X_test)

            f1_test = f1_score(y_test, y_test_pred, average='weighted')
            performance_report[model_name] = f1_test

        return performance_report

    except Exception as e:
        raise NetworkSecurityException(e, sys)
