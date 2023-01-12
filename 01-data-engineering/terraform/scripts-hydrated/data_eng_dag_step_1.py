# ............................................................
# Preprocessing - Step 1
# ............................................................
# This script performs data preprocessing (step 1) on raw data in GCS
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
    sourceBucketUri = f"gs://s8s_data_bucket-{projectNbr}/telco_customer_churn_train_data.csv"
    bigQueryTargetTableFQN = f"{bqDatasetNm}.training_data_step_1"
    pipelineExecutionDt = datetime.now().strftime("%Y%m%d%H%M%S")

    # 1c. Display input and output
    if displayPrintStatements:
        logger.info("Starting STEP 1 data preprocessing for the *Customer Churn* pipeline")
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
        logger.info(f"....Source Data={sourceBucketUri}")
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
        rawChurnDF = spark.read.options(inferSchema = True, header= True).csv(sourceBucketUri)

        nullsReplacedDF=rawChurnDF.select([when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            (col(c) == ' ')  | \
                            col(c).isNull() | \
                            isnan(c),None).otherwise(col(c)).alias(c) for c in rawChurnDF.columns])


        logger.info('....Persist to BQ')

        persistDF = nullsReplacedDF.select("customerID", "gender", "SeniorCitizen", "Partner", "Dependents", "tenure", "PhoneService", "MultipleLines", "InternetService", "OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies", "Contract", "PaperlessBilling", "PaymentMethod", "MonthlyCharges", "TotalCharges","Churn") \
                                        .toDF("customer_id", "gender", "senior_citizen", "partner", "dependents", "tenure", "phone_service", "multiple_lines", "internet_service", "online_security", "online_backup", "device_protection", "tech_support", "streaming_tv", "streaming_movies", "contract", "paperless_billing", "payment_method", "monthly_charges", "total_charges","churn") \
                                        .withColumn("pipeline_id", lit(pipelineID))

        persistDF.write.format('bigquery') \
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