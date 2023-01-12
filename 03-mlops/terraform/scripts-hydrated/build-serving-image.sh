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
GIT_BIN=`which git`



LOG_DATE=`date`
echo "###########################################################################################"
echo "${LOG_DATE}  SPARK Hackfest - Build serving image  .."

if [ "${#}" -ne 1 ]; then
    echo "Illegal number of parameters. Exiting ..."
    echo "Usage: ${0} <GCP_REGION>"
    echo "Example: ${0} us-central1"
    echo "Exiting ..."
    exit 1
fi


# Variables
PROJECT_ID=`"${GCLOUD_BIN}" config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`"${GCLOUD_BIN}" projects describe ${PROJECT_ID} | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
LOCAL_SCRATCH_DIR=~/build
REPO_NAME="s8s-spark-${PROJECT_ID}"
GCP_REGION=${1}

# Create local directory
cd ~
mkdir build
cd build
rm -rf *
echo "Created local directory for the Docker image building"

# Create Dockerfile in local directory
cd ${LOCAL_SCRATCH_DIR}

${GIT_BIN} clone https://github.com/GoogleCloudPlatform/cloud-builders-community.git
cd cloud-builders-community/scala-sbt && ${GCLOUD_BIN} builds submit .
cd ..
${GIT_BIN} clone https://github.com/GoogleCloudPlatform/vertex-ai-spark-ml-serving.git
cd vertex-ai-spark-ml-serving && ${GCLOUD_BIN} builds submit --config=cloudbuild.yaml --substitutions="_LOCATION=${GCP_REGION},_REPOSITORY=${REPO_NAME},_IMAGE=spark_ml_serving" .

