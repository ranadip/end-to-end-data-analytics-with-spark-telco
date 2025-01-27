# End to end data analytics with SPARK (Telco edition)

## Introduction

This repository contains a refactoring of [s8s-spark-ce-workshop](https://github.com/anagha-google/s8s-spark-ce-workshop) into three different labs that show three different stages of a data analytics journey:

1. [Data Engineering](01-data-engineering)
2. [Machine Learning](02-ml)
3. [MLOps](03-mlops)


## Architecture 

[Architecture and narrative](assets/end_to_end_data_analytics_with_SPARK.pdf)

## Installation

The deployment is fully terraformed. For each lab and from a [Google Cloud Cloud Shell](https://cloud.google.com/shell) terminal logged as your admin user, execute the following commands:


```console
~$ cd <LAB_NAME>
# e.g. where <LAB_NAME> is 01-data-engineering, 02-ml or 03-mlops
~$ source local_project_launcher.sh <gcp_project_id> <gcp_region> <gcp_zone> <gcp_user_id>
```

Change `<gcp_project_id> <gcp_region> <gcp_zone> <gcp_user_id>` accordingly, for example:

```console
~$ source local_project_launcher.sh ${GOOGLE_CLOUD_PROJECT} us-central1 us-central1-a velascoluis@google.com
```

Follow instructions for each lab under `<LAB_NAME>/instructions/en.md` 




