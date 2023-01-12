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
GSUTIL_BIN=`which gsutil`

LOG_DATE=`date`
echo "###########################################################################################"
echo "${LOG_DATE}  SPARK Hackfest - upload trainer file  .."

if [ ! "${CLOUD_SHELL}" = true ] ; then
    echo "This script needs to run on Google Cloud Shell. Exiting ..."
    exit 1
fi

PROJECT_ID=`"${GCLOUD_BIN}" config list --format "value(core.project)" 2>/dev/null`
echo "PROJECT_ID : ${PROJECT_ID}"
PROJECT_NBR=`"${GCLOUD_BIN}" projects describe ${PROJECT_ID} | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
echo "PROJECT_NBR : ${PROJECT_NBR}"

"${GSUTIL_BIN}" cp model_building.py "gs://s8s_code_bucket-"${PROJECT_ID}"/modelbuilding/"

LOG_DATE=`date`
echo "###########################################################################################"
echo "${LOG_DATE} Execution finished! ..."

