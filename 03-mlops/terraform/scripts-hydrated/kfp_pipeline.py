# ............................................................
# Vertex pipeline training + model deployment
# ............................................................
# This defines a Vertex AI Pipeline using the KFP DSL, 
# that launches a training and deploys a online model
# in Vertex AI
# ............................................................

import random
from pathlib import Path as path
from typing import NamedTuple
import os
import logging
import argparse
import sys
from datetime import datetime

from google.cloud import aiplatform as vertex_ai
from google_cloud_pipeline_components import aiplatform as vertex_ai_components
from kfp.v2 import compiler, dsl
from kfp.v2.dsl import (Artifact, ClassificationMetrics, Condition, Input,
                        Metrics, Output, component)


@component(
    base_image="python:3.8-slim",
    packages_to_install=["google-cloud-aiplatform"],
)
def import_model_artifact(
    model: dsl.Output[dsl.Artifact], artifact_uri: str, serving_image_uri: str
):
    model.metadata["containerSpec"] = {
        "imageUri": serving_image_uri,
        "healthRoute": "/health",
        "predictRoute": "/predict",
    }
    model.uri = artifact_uri

@component(
    base_image="python:3.8",
    packages_to_install=["google-cloud-bigquery-storage","pyarrow"]
)
def get_evaluation_metric(
    project_id: str,
    pipeline_id: str,
) -> NamedTuple("Outputs", [("metric", float)]):

    from google.cloud.bigquery_storage import BigQueryReadClient
    from google.cloud.bigquery_storage import types
    client = BigQueryReadClient()
    table = "projects/{}/datasets/{}/tables/{}".format(
    project_id, "customer_churn_ds", "model_metrics")
    requested_session = types.ReadSession()
    requested_session.table = table
    requested_session.data_format = types.DataFormat.ARROW
    requested_session.read_options.selected_fields = ["metric_value"]
    requested_session.read_options.row_restriction = 'pipeline_id = \'{}\''.format(pipeline_id)
    session = client.create_read_session(
        parent="projects/{}".format(project_id),
        read_session=requested_session,
        max_stream_count=1,
)
    reader = client.read_rows(session.streams[0].name)
    rows = reader.rows(session)
    metric_value = 0
    for row in rows:
        metric_value = row['metric_value']
    component_outputs = NamedTuple(
        "Outputs",
        [
            ("metric", float),
        ],
    )
    return component_outputs(metric_value.cast('float').as_py())


@dsl.pipeline(name="pyspark-churn-model-pipeline", description="A pipeline to train and deploy PySpark model.")
def fnSparkMlopsPipeline(
    project_id : str,
    pipeline_id: str,
    location : str,
    service_account : str,
    subnetwork_uri : str,
    spark_phs_nm : str,
    container_image : str,
    managed_dataset_display_nm : str,
    managed_dataset_src_uri : str,
    model_training_pyspark_batch_id : str,
    model_training_main_py_fqn : str,
    model_training_args : list,
    threshold : float,
    deploy_model : bool,
    model_artifact_uri : str,
    serving_image_uri : str
    
):
    from google_cloud_pipeline_components.v1.dataproc import \
        DataprocPySparkBatchOp
    from google_cloud_pipeline_components.v1.dataset import \
        TabularDatasetCreateOp
    from google_cloud_pipeline_components.v1.endpoint import (EndpointCreateOp,
                                                              ModelDeployOp)
    from google_cloud_pipeline_components.v1.model import ModelUploadOp

    createManagedDatasetStep = vertex_ai_components._______INSERT_CODE_HERE_______(
        display_name= managed_dataset_display_nm,
        bq_source=managed_dataset_src_uri,
        project=project_id,
        location=location,
    ).set_display_name("Dataset registration")
    
    trainSparkMLModelStep = _______INSERT_CODE_HERE_______(
        project = project_id,
        location = location,
        container_image = container_image,
        subnetwork_uri = subnetwork_uri,
        spark_history_dataproc_cluster = spark_phs_nm,
        service_account = service_account,     
        batch_id = model_training_pyspark_batch_id,
        main_python_file_uri = model_training_main_py_fqn,
        jar_file_uris = ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"],
        runtime_config_properties = {
    "spark.jars.packages": "ml.combust.mleap:mleap-spark-base_2.12:0.20.0,ml.combust.mleap:mleap-spark_2.12:0.20.0"
},
        args = model_training_args
    ).after(_______INSERT_CODE_HERE_______).set_display_name("Model training")

    evaluateModelStep = get_evaluation_metric(project_id=project_id,pipeline_id=pipeline_id).after(trainSparkMLModelStep)

    with Condition(
        _______INSERT_CODE_HERE_______,
        name="deploy_condition",
    ):
        importModelArtifactStep = import_model_artifact(
                artifact_uri=model_artifact_uri,
                serving_image_uri=serving_image_uri)
        modelUploadStep = _______INSERT_CODE_HERE_______(
                project=project_id,
                location=location,
                display_name="pyspark-customer-churn-RF",
                unmanaged_container_model=importModelArtifactStep.outputs["model"],
            ).after(_______INSERT_CODE_HERE_______)
        endpointStep = _______INSERT_CODE_HERE_______(
                project=project_id,
                location=location,
                display_name="pyspark-customer-churn-RF"
            ).after(_______INSERT_CODE_HERE_______)
        _ = _______INSERT_CODE_HERE_______(
                model=modelUploadStep.outputs["model"],
                endpoint=endpointStep.outputs["endpoint"],
                dedicated_resources_machine_type="n1-standard-2",
                dedicated_resources_min_replica_count=1,
                dedicated_resources_max_replica_count=1,
            ).after(_______INSERT_CODE_HERE_______)




def fnParseArguments():
# {{ Start 
    """
    Purpose:
        Parse arguments received by script
    Returns:
        args
    """
    argsParser = argparse.ArgumentParser()
    argsParser.add_argument(
        '--pipelineID',
        help='Unique ID for the pipeline stages for traceability',
        type=str,
        required=True)
    argsParser.add_argument(
        '--projectNbr',
        help='The project number',
        type=str,
        required=True)
    argsParser.add_argument(
        '--gcpRegion',
        help='The GCP region number',
        type=str,
        required=True)    
    argsParser.add_argument(
        '--projectID',
        help='The project id',
        type=str,
        required=True)
    return argsParser.parse_args()
# }} End fnParseArguments()

def fnMain(logger, args):
# {{ Start main
        
    pipelineID = args.pipelineID
    projectNbr = args.projectNbr
    projectID = args.projectID
    gcpRegion = args.gcpRegion
    pipelineExecutionDt = datetime.now().strftime("%Y%m%d%H%M%S")

    logger.info("Starting model training for *Customer Churn* experiment")
    logger.info(".....................................................")
    logger.info(f"The datetime now is - {pipelineExecutionDt}")
    logger.info(" ")
    logger.info("INPUT PARAMETERS")
    logger.info(f"....pipelineID={pipelineID}")
    logger.info(f"....projectID={projectID}")
    logger.info(f"....projectNbr={projectNbr}")
    logger.info(f"....gcpRegion={gcpRegion}")
        
    
    package_path = "pipeline_{}.json".format(pipelineID)

    try:
        compiler.Compiler().compile(pipeline_func=fnSparkMlopsPipeline, package_path=package_path)
        pipeline = vertex_ai.PipelineJob(
            display_name="customer-churn-model-pipeline",
            template_path=package_path,
            pipeline_root='gs://s8s_model_bucket-{}/customer-churn-model/pipelines'.format(projectID),
            parameter_values={
                'project_id': projectID,
                'pipeline_id': pipelineID,
                'location': gcpRegion,
                'service_account': 's8s-lab-sa@{}.iam.gserviceaccount.com'.format(projectID),
                'subnetwork_uri': 'projects/{}/regions/{}/subnetworks/spark-snet'.format(projectID,gcpRegion),
                'spark_phs_nm': 'projects/{}/regions/{}/clusters/s8s-sphs-{}'.format(projectID,gcpRegion,projectID),
                'container_image': 'gcr.io/{}/customer_churn_image:1.0.0'.format(projectID),
                'managed_dataset_display_nm': 'customer_churn_managed_training_dataset',
                'managed_dataset_src_uri': 'bq://{}.customer_churn_ds.training_data_step_3'.format(projectID),
                'model_training_pyspark_batch_id': 'model-building-{}-{}'.format(pipelineID,random.randint(1, 10000)),
                'model_training_main_py_fqn': 'gs://s8s_code_bucket-{}/model_building.py'.format(projectID),
                'model_training_args': ['--pipelineID={}'.format(pipelineID),'--projectID={}'.format(projectID), '--projectNbr={}'.format(projectNbr), '--displayPrintStatements={}'.format(True)],
                'threshold': 0.6,
                'deploy_model': True,
                'model_artifact_uri': 'gs://s8s_model_bucket-{}/customer-churn-model/training/{}/bundle/'.format(projectNbr,pipelineID),
                'serving_image_uri' : '{}-docker.pkg.dev/{}/s8s-spark-{}/spark_ml_serving:latest'.format(gcpRegion,projectID,projectNbr)
                },
            enable_caching=_______INSERT_CODE_HERE_______,
        )
        umsa_fqn="s8s-lab-sa@{}.iam.gserviceaccount.com".format(projectID)
        pipeline.submit(service_account=umsa_fqn, network="projects/{}/global/networks/s8s-vpc-{}".format(projectNbr,projectID))
        pipeline.wait()

    except RuntimeError as coreError:
            logger.error(coreError)
    else:
        logger.info('Successfully completed model training!')

# }} End fnMain()

def fnConfigureLogger():
# {{ Start 
    """
    Purpose:
        Configure a logger for the script
    Returns:
        Logger object
    """
    logFormatter = logging.Formatter('%(asctime)s - %(filename)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("vertex_pipeline")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logStreamHandler = logging.StreamHandler(sys.stdout)
    logStreamHandler.setFormatter(logFormatter)
    logger.addHandler(logStreamHandler)
    return logger
# }} End fnConfigureLogger()

if __name__ == "__main__":
# {{ Entry point
    arguments = fnParseArguments()
    logger = fnConfigureLogger()
    fnMain(logger, arguments)

# }} End enty point