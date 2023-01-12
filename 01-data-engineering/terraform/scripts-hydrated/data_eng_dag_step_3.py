# ............................................................
# Preprocessing - Step 3
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
    bigQuerySourceTableFQN = f"{bqDatasetNm}.training_data_step_2"
    bigQueryTargetTableFQN = f"{bqDatasetNm}.training_data_step_3"
    pipelineExecutionDt = datetime.now().strftime("%Y%m%d%H%M%S")

    # 1c. Display input and output
    if displayPrintStatements:
        logger.info("Starting STEP 3.....................................")
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

        partiallyProcessedDF = spark.read.format('bigquery') \
        .option("table", f"{bigQuerySourceTableFQN}").load()

        partiallyProcessedDF.createOrReplaceTempView("partially_transformed_customer_churn")
        modelTrainingReadyDF = spark.sql("""
                                select  customer_id 
                                        ,gender 
                                        ,cast(senior_citizen as int) senior_citizen
                                        ,partner
                                        ,dependents
                                        ,cast(tenure as int) tenure
                                        ,case when (tenure<=12) then "Tenure_0-12"
                                              when (tenure>12 and tenure <=24) then "Tenure_12-24"
                                              when (tenure>24 and tenure <=48) then "Tenure_24-48"
                                              when (tenure>48 and tenure <=60) then "Tenure_48-60"
                                              when (tenure>60) then "Tenure_gt_60"
                                        end as tenure_group
                                        ,phone_service
                                        ,multiple_lines
                                        ,internet_service
                                        ,online_security
                                        ,online_backup
                                        ,device_protection
                                        ,tech_support
                                        ,streaming_tv
                                        ,streaming_movies
                                        ,contract
                                        ,paperless_billing
                                        ,payment_method
                                        ,cast(monthly_charges as float) monthly_charges
                                        ,cast(total_charges as float) total_charges
                                        ,lcase(churn) as churn
                                        ,pipeline_id
                                from partially_transformed_customer_churn  
                                """)

        logger.info('....Persist to BQ')  
        modelTrainingReadyDF.write.format('bigquery') \
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