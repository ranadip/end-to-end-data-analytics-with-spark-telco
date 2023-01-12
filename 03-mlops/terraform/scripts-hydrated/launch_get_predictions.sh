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
PYTHON_BIN=`which python3`
PIP_BIN=`which pip3`
JQ_BIN=`which jq`

LOG_DATE=`date`
echo "###########################################################################################"
echo "${LOG_DATE}  SPARK Hackfest - launch predictions   .."


if [ ! "${CLOUD_SHELL}" = true ] ; then
    echo "This script needs to run on Google Cloud Shell. Exiting ..."
    exit 1
fi





PROJECT_ID=`"${GCLOUD_BIN}" config list --format "value(core.project)" 2>/dev/null`
GCP_REGION=`"${GCLOUD_BIN}" compute project-info describe --project ${PROJECT_ID} --format "value(commonInstanceMetadata.google-compute-default-region)" 2>/dev/null`

echo "PROJECT_ID : ${PROJECT_ID}"
MODEL_NAME="pyspark-customer-churn-RF"
echo "MODEL_NAME : ${MODEL_NAME}"
ENDPOINT_ID=`"${GCLOUD_BIN}" ai endpoints list \
              --region=${GCP_REGION} \
              --filter=display_name=${MODEL_NAME} \
              --format='value(name)'`
echo "ENDPOINT_ID : ${ENDPOINT_ID}"

${PYTHON_BIN} -m venv local_test_env
source local_test_env/bin/activate
${PIP_BIN} install -r requirements.txt
${PYTHON_BIN} get_predictions.py --projectID ${PROJECT_ID} --endpointID ${ENDPOINT_ID} --location ${GCP_REGION}



 