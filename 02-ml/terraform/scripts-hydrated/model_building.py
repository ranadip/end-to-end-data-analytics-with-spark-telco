# ............................................................
# Model training
# ............................................................
# This script does model training, with Spark MLLib
# Supervised learning, binary classification with 
# RandomForestClassifier and uses the Spark MLLib pipeline API
# Uses BigQuery as a source, and writes test results and metrics
# to BigQuery
# ............................................................
import mleap.pyspark
from mleap.pyspark.spark_support import _______INSERT_CODE_HERE_______
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import  StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
import pandas as pd
import sys, logging, argparse, random, tempfile, json
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import StructType, DoubleType, StringType
from pyspark.sql.functions import lit
from pathlib import Path as path
from google.cloud import storage
from urllib.parse import urlparse, urljoin
from datetime import datetime


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
        '--projectID',
        help='The project id',
        type=str,
        required=True)
    argsParser.add_argument(
        '--displayPrintStatements',
        help='Boolean - print to screen or not',
        type=bool,
        required=True)
    return argsParser.parse_args()
# }} End fnParseArguments()

def fnMain(logger, args):
# {{ Start main

    # 1a. Arguments
    pipelineID = args.pipelineID
    projectNbr = args.projectNbr
    projectID = args.projectID
    displayPrintStatements = args.displayPrintStatements

    # 1b. Variables 
    appBaseName = "customer-churn-model"
    appNameSuffix = "training"
    appName = f"{appBaseName}-{appNameSuffix}"
    modelBaseNm = appBaseName
    modelVersion = pipelineID
    bqDatasetNm = f"{projectID}.customer_churn_ds"
    operation = appNameSuffix
    bigQuerySourceTableFQN = f"{bqDatasetNm}._______INSERT_CODE_HERE_______"
    bigQueryModelTestResultsTableFQN = f"{bqDatasetNm}.test_predictions"
    bigQueryModelMetricsTableFQN = f"{bqDatasetNm}.model_metrics"
    modelBucketUri = f"gs://s8s_model_bucket-{projectID}/{modelBaseNm}/{operation}/{modelVersion}"
    scratchBucketUri = f"s8s-spark-bucket-{projectID}/{appBaseName}/pipelineId-{pipelineID}/{appNameSuffix}/"
    pipelineExecutionDt = datetime.now().strftime("%Y%m%d%H%M%S")

    # Other variables, constants
    SPLIT_SEED = 6
    SPLIT_SPECS = [0.8, 0.2]

    # 1c. Display input and output
    if displayPrintStatements:
        logger.info("Starting model training for *Customer Churn* experiment")
        logger.info(".....................................................")
        logger.info(f"The datetime now is - {pipelineExecutionDt}")
        logger.info(" ")
        logger.info("INPUT PARAMETERS")
        logger.info(f"....pipelineID={pipelineID}")
        logger.info(f"....projectID={projectID}")
        logger.info(f"....projectNbr={projectNbr}")
        logger.info(f"....displayPrintStatements={displayPrintStatements}")
        logger.info(" ")
        logger.info("EXPECTED SETUP")  
        logger.info(f"....BQ Dataset={bqDatasetNm}")
        logger.info(f"....Model Training Source Data in BigQuery={bigQuerySourceTableFQN}")
        logger.info(f"....Scratch Bucket for BQ connector=gs://s8s-spark-bucket-{projectID}") 
        logger.info(f"....Model Bucket=gs://s8s-model-bucket-{projectID}")  
        logger.info(" ")
        logger.info("OUTPUT")
        logger.info(f"....Model and bundle in GCS={modelBucketUri}")
        logger.info(f"....Model metrics in BigQuery={bigQueryModelMetricsTableFQN}")      
        logger.info(f"....Model test results in BigQuery={bigQueryModelTestResultsTableFQN}") 

    try:
        
        logger.info('....Initializing spark & spark configs')
        spark = SparkSession.builder.appName(appName).getOrCreate()
        spark.conf.set("parentProject", projectID)
        spark.conf.set("temporaryGcsBucket", scratchBucketUri)
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        logger.info('....Read the training dataset into a dataframe')
        inputDF = spark.read \
            .format('bigquery') \
            .load(bigQuerySourceTableFQN)

        inputDF = inputDF.withColumn("monthly_charges", inputDF.monthly_charges.cast('float')) \
            .withColumn("total_charges", inputDF.total_charges.cast('float'))

        if displayPrintStatements:
            inputDF.printSchema()
            logger.info(f"inputDF count={inputDF.count()}")

        logger.info('....Split the dataset')
        trainDF, testDF = inputDF.randomSplit(SPLIT_SPECS, seed=SPLIT_SEED)
        logger.info('....Data pre-procesing')
        dataPreprocessingStagesList = []
        CATEGORICAL_COLUMN_LIST = ['gender', 'senior_citizen', 'partner', 'dependents', 'phone_service', 'multiple_lines',
                        'internet_service', 'online_security', 'online_backup', 'device_protection', 'tech_support',
                        'streaming_tv', 'streaming_movies', 'contract', 'paperless_billing', 'payment_method'] 

        for eachCategoricalColumn in CATEGORICAL_COLUMN_LIST:
            stringIndexer = StringIndexer(inputCol=eachCategoricalColumn, outputCol=eachCategoricalColumn + "Index")
            if (spark.version).startswith("2."):
                from pyspark.ml.feature import OneHotEncoderEstimator
                encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[eachCategoricalColumn + "classVec"])
            elif (spark.version).startswith("3."):
                from pyspark.ml.feature import OneHotEncoder
                encoder = OneHotEncoder(inputCols=[stringIndexer.getOutputCol()], outputCols=[eachCategoricalColumn + "classVec"])
            else:
                from pyspark.ml.feature import OneHotEncoder
                encoder = OneHotEncoder(inputCols=[stringIndexer.getOutputCol()], outputCols=[eachCategoricalColumn + "classVec"])
            dataPreprocessingStagesList += [stringIndexer, encoder]
        labelStringIndexer = StringIndexer(inputCol="churn", outputCol="label")
        dataPreprocessingStagesList += [labelStringIndexer]

      
        NUMERIC_COLUMN_LIST = ['monthly_charges', 'total_charges']
        assemblerInputs = NUMERIC_COLUMN_LIST + [c + "classVec" for c in CATEGORICAL_COLUMN_LIST]
        featuresVectorAssembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
        dataPreprocessingStagesList += [featuresVectorAssembler]
        logger.info('....Model training')
        modelTrainingStageList = []
        rfClassifier = RandomForestClassifier(labelCol="label", featuresCol="features")
        modelTrainingStageList += [rfClassifier]
        logger.info('....Instantiating pipeline model')
        pipeline = Pipeline(stages=dataPreprocessingStagesList +  modelTrainingStageList)   
        logger.info('....Fit the model')
        pipelineModel = pipeline.fit(trainDF)
        logger.info('....Persist the model to GCS')
        pipelineModel._______INSERT_CODE_HERE_______
        logger.info('....Test the model')
        predictionsDF = pipelineModel.transform(testDF)
        predictionsDF.show(2)
        persistPredictionsDF = predictionsDF.withColumn("pipeline_id", lit(pipelineID).cast("string")) \
                                        .withColumn("model_version", lit(pipelineID).cast("string")) \
                                        .withColumn("pipeline_execution_dt", lit(pipelineExecutionDt)) \
                                        .withColumn("operation", lit(operation)) 

        persistPredictionsDF.write.format('bigquery') \
        .mode("append")\
        .option('table', bigQueryModelTestResultsTableFQN) \
        .save()
        logger.info('....Calculating area under the ROC curve')
        evaluator = BinaryClassificationEvaluator()
        evaluator.setRawPredictionCol("prediction")
        evaluator.setLabelCol("label")
        value = evaluator.evaluate(predictionsDF, {evaluator.metricName: "areaUnderROC"})       
        metricsDF = spark.createDataFrame( [("areaUnderROC",value)], ["metric_nm", "metric_value"]) 
        metricsWithPipelineIdDF = metricsDF.withColumn("pipeline_id", lit(pipelineID).cast("string")) \
                                        .withColumn("model_version", lit(pipelineID).cast("string")) \
                                        .withColumn("pipeline_execution_dt", lit(pipelineExecutionDt)) \
                                        .withColumn("operation", lit(appNameSuffix)) 

        metricsWithPipelineIdDF.show()
        metricsWithPipelineIdDF.write.format('bigquery') \
        .mode("append")\
        .option('table', bigQueryModelMetricsTableFQN) \
        .save()
        logger.info('....Serialize the model bundle and upload to GCS')
        pipelineModel.serializeToBundle(_______INSERT_CODE_HERE_______)

        storage_client = storage.Client()
        bundle_file_path = urlparse(f"{modelBucketUri}/bundle/model.zip").path.strip('/')
        bucket = urlparse(f"{modelBucketUri}/bundle/model.zip").netloc
        bucket = storage_client.bucket(bucket)
        blob = bucket.blob(bundle_file_path)
        blob.upload_from_filename(_______INSERT_CODE_HERE_______)


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
    logger = logging.getLogger("data_engineering")
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