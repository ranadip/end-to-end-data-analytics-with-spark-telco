#!/bin/sh
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


GCLOUD_BIN=`which gcloud`
JQ_BIN=`which jq`


LOG_DATE=`date`
echo "###########################################################################################"
echo "${LOG_DATE}  SPARK Hackfest - launch model building  .."



if [ ! "${CLOUD_SHELL}" = true ] ; then
    echo "This script needs to run on Google Cloud Shell. Exiting ..."
    exit 1
fi


PIPELINE_ID=${RANDOM}
PROJECT_ID=`"${GCLOUD_BIN}" config list --format "value(core.project)" 2>/dev/null`
echo "PROJECT_ID : ${PROJECT_ID}"
PROJECT_NBR=`"${GCLOUD_BIN}" projects describe ${PROJECT_ID} | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
echo "PROJECT_NBR : ${PROJECT_NBR}"
GCP_REGION=`"${GCLOUD_BIN}" compute project-info describe --project ${PROJECT_ID} --format "value(commonInstanceMetadata.google-compute-default-region)" 2>/dev/null`
echo "GCP_REGION : ${GCP_REGION}"
PYSPARK_CODE="gs://s8s_code_bucket-${PROJECT_ID}/modelbuilding/model_building.py"
echo "PYSPARK_CODE : ${PYSPARK_CODE}"
SUBNET="spark-snet"
echo "SUBNET : ${SUBNET}"
UMSA_FQN="s8s-lab-sa@${PROJECT_ID}.iam.gserviceaccount.com"
echo "UMSA_FQN : ${UMSA_FQN}"
SPARK_CUSTOM_CONTAINER_IMAGE="gcr.io/${PROJECT_ID}/customer_churn_image:1.0.0"
echo "PROJECTSPARK_CUSTOM_CONTAINER_IMAGE_NBR : ${SPARK_CUSTOM_CONTAINER_IMAGE}"
BQ_CONNECTOR_JAR_GCS_URI="gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"
echo "BQ_CONNECTOR_JAR_GCS_URI : ${BQ_CONNECTOR_JAR_GCS_URI}"
PERSISTENT_HISTORY_SERVER_NM="projects/"${PROJECT_ID}"/regions/"${GCP_REGION}"/clusters/s8s-sphs-${PROJECT_ID}"
echo "PERSISTENT_HISTORY_SERVER_NM : ${PERSISTENT_HISTORY_SERVER_NM}"
BATCH_UUID=model-building-${PIPELINE_ID}-${RANDOM}
echo "BATCH_UUID : ${BATCH_UUID}"
RUNTIME_PROPERTIES="^#^spark.jars.packages=ml.combust.mleap:mleap-spark-base_2.12:0.20.0,ml.combust.mleap:mleap-spark_2.12:0.20.0"
echo "RUNTIME_PROPERTIES : ${RUNTIME_PROPERTIES}"

${GCLOUD_BIN} dataproc batches submit pyspark ${PYSPARK_CODE} --batch=${BATCH_UUID}  --region=${GCP_REGION} --history-server-cluster=${PERSISTENT_HISTORY_SERVER_NM} --properties=${RUNTIME_PROPERTIES}  --subnet=${SUBNET} --service-account=${UMSA_FQN} --container-image=${SPARK_CUSTOM_CONTAINER_IMAGE} --jars=${BQ_CONNECTOR_JAR_GCS_URI} -- --pipelineID=${PIPELINE_ID} --projectNbr=${PROJECT_NBR} --projectID=${PROJECT_ID} --displayPrintStatements=True