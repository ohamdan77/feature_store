# Databricks notebook source
# MAGIC %md ### Apply model to new records

# COMMAND ----------

import mlflow.spark
from mlflow.tracking import MlflowClient
from databricks.feature_store import FeatureStoreClient

client = MlflowClient()
fs = FeatureStoreClient()

# COMMAND ----------

# MAGIC %md Simulate new records; Notice that only the record IDs need to be passes. The MLflow model has recorded the feature looking logic and will join the necessary features to the record Ids.

# COMMAND ----------

new_passenger_records = (spark.table('ohamdan_fs_demo.passenger_labels')
                              .select('PassengerId')
                              .limit(20))

display(new_passenger_records)

# COMMAND ----------

# MAGIC %md Get model's unique identifier

# COMMAND ----------

def get_run_id(model_name, stage='Production'):
  """Get production model id from Model Registry"""
  
  prod_run = [run for run in client.search_model_versions(f"name='{model_name}'") 
                  if run.current_stage == stage][0]
  
  return prod_run.run_id


# Replace the first parameter with your model's name
run_id = get_run_id('oh_feature_store_models', stage='Production')
run_id

# COMMAND ----------

# MAGIC %md Score records

# COMMAND ----------

mod_uri = "dbfs:/databricks/mlflow-tracking/4360330468110816/3a4f058c68de4445a0986ad06f67684a/artifacts/model"
model_uri = f'runs:/{run_id}/model'

with_predictions = fs.score_batch(model_uri, new_passenger_records)

display(with_predictions)
