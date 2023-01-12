# ............................................................
# Preprocessing - Step 2
# ............................................................
# This script performs data preprocessing (step 2) from intermediate data in BigQuery
# and persists to BigQuery
# ............................................................

import sys,logging,argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

    # 1. Capture Spark application input
    pipelineID = args.pipelineID
    projectNbr = args.projectNbr
    projectID = args.projectID
    displayPrintStatements = args.displayPrintStatements

    # 1b. Variables 
    bqDatasetNm = f"{projectID}.customer_churn_ds"
    appBaseName = "customer-churn-model"
    appNameSuffix = "preprocessing"
    appName = f"{appBaseName}-{appNameSuffix}"
    scratchBucketUri = f"s8s-spark-bucket-{projectNbr}/{appBaseName}/pipelineId-{pipelineID}/{appNameSuffix}"
    bigQuerySourceTableFQN = f"{bqDatasetNm}.training_data_step_1"
    bigQueryTargetTableFQN = f"{bqDatasetNm}.training_data_step_2"
    pipelineExecutionDt = datetime.now().strftime("%Y%m%d%H%M%S")

    # 1c. Display input and output
    if displayPrintStatements:
        logger.info("Starting STEP 2 data preprocessing for the *Customer Churn* pipeline")
        logger.info(".....................................................")
        logger.info(f"The datetime now is - {pipelineExecutionDt}")
        logger.info(" ")
        logger.info("INPUT PARAMETERS-")
        logger.info(f"....pipelineID={pipelineID}")
        logger.info(f"....projectID={projectID}")
        logger.info(f"....projectNbr={projectNbr}")
        logger.info(f"....displayPrintStatements={displayPrintStatements}")
        logger.info(" ")
        logger.info("EXPECTED SETUP-")  
        logger.info(f"....BQ Dataset={bqDatasetNm}")
        logger.info(f"....Source Data={bigQuerySourceTableFQN}")
        logger.info(f"....Scratch Bucket for BQ connector=gs://s8s-spark-bucket-{projectNbr}") 
        logger.info("OUTPUT-")
        logger.info(f"....BigQuery Table={bigQueryTargetTableFQN}")
        logger.info(f"....Sample query-")
        logger.info(f"....SELECT * FROM {bigQueryTargetTableFQN} WHERE pipeline_id='{pipelineID}' LIMIT 10" )

    try:
       
        logger.info('....Initializing spark & spark configs')
        spark = SparkSession.builder.appName(appName).getOrCreate()

        
        spark.conf.set("parentProject", projectID)
        spark.conf.set("temporaryGcsBucket", scratchBucketUri)

        
        logger.info('....Read source data')

        nullsReplacedDF = spark.read.format('bigquery') \
        .option("table", f"{bigQuerySourceTableFQN}").load()

        nullDroppedDF = nullsReplacedDF.na.drop()


        partiallyProcessedDF = nullDroppedDF.select([when( (col(c) == "No internet service") | (col(c) == "No phone service") , "No").otherwise(col(c)).alias(c) for c in nullDroppedDF.columns])

        logger.info('....Persist to BQ')  
        partiallyProcessedDF.write.format('bigquery') \
        .mode("overwrite")\
        .option('table', bigQueryTargetTableFQN) \
        .save()

    except RuntimeError as coreError:
            logger.error(coreError)
    else:
        logger.info('Successfully completed preprocessing!')
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
    arguments = fnParseArguments()
    logger = fnConfigureLogger()
    fnMain(logger, arguments)