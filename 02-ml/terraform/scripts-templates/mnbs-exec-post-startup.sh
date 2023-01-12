#!/bin/bash

#........................................................................
# Purpose: Copy existing notebooks to Workbench server Jupyter home dir
# (Managed notebook server)
#........................................................................

gsutil cp gs://s8s_code_bucket-YOUR_PROJECT_NBR/*.ipynb /home/jupyter/ 
#sudo chown jupyter:jupyter /home/jupyter/* 
