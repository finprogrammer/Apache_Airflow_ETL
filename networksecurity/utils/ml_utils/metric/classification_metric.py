from networksecurity.entity.artifact_entity import ClassificationMetricArtifact
from networksecurity.exception.exception import NetworkSecurityException
from sklearn.metrics import (
    f1_score, precision_score, recall_score, classification_report, confusion_matrix
)
import sys
import logging

def get_classification_score(y_true, y_pred, average='macro') -> ClassificationMetricArtifact:
    try:
        model_f1_score = f1_score(y_true, y_pred, average=average)
        model_recall_score = recall_score(y_true, y_pred, average=average)
        model_precision_score = precision_score(y_true, y_pred, average=average)

        logging.info("Classification Report:\n" + classification_report(y_true, y_pred))
        logging.info("Confusion Matrix:\n" + str(confusion_matrix(y_true, y_pred)))

        classification_metric = ClassificationMetricArtifact(
            f1_score=model_f1_score,
            precision_score=model_precision_score,
            recall_score=model_recall_score
        )
        return classification_metric
    except Exception as e:
        raise NetworkSecurityException(e, sys)
