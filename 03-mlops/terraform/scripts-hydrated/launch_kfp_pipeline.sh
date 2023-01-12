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
PYTHON_BIN=`which python3`
PIP_BIN=`which pip3`
GSUTIL_BIN=`which gsutil`


LOG_DATE=`date`
echo "###########################################################################################"
echo "${LOG_DATE}  SPARK Hackfest - launch KFP pipeline  .."



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

LOG_DATE=`date`
echo "###########################################################################################"
echo "${LOG_DATE}  SPARK Hackfest - Uploading schema file  .."

"${GSUTIL_BIN}" cp schema.json "gs://s8s_model_bucket-"${PROJECT_ID}"/customer-churn-model/training/"${PIPELINE_ID}"/bundle/schema.json"

LOG_DATE=`date`
echo "###########################################################################################"
echo "${LOG_DATE}  SPARK Hackfest - calling KFP pipeline  .."

${PYTHON_BIN} -m venv local_test_env
source local_test_env/bin/activate
${PIP_BIN} install -r requirements.txt
${PYTHON_BIN} kfp_pipeline.py --pipelineID ${PIPELINE_ID}  --projectNbr ${PROJECT_NBR} --gcpRegion ${GCP_REGION} --projectID ${PROJECT_ID}
