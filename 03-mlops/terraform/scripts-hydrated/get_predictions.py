# ............................................................
# Get predictions
# ............................................................
# This script launches some random predictions aganst a Vertex AI Endpoint
# where the SPARK MLib model is deployed for online inference
# ............................................................

import argparse
import asyncio
import random
import logging
import sys

from google.api_core.client_options import ClientOptions
from google.cloud import aiplatform_v1
from google.cloud.aiplatform_v1.types import PredictRequest
from google.protobuf import struct_pb2



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
        '--projectID',
        help='The project id',
        type=str,
        required=True)
    argsParser.add_argument(
        '--endpointID',
        help='The vertex AI endpoint id',
        type=str,
        required=True)        
    argsParser.add_argument(
        '--location',
        help='The vertex AI location',
        type=str,
        required=True)
    argsParser.add_argument(
        '--numInstances',
        help='The number of random instances to generate',
        type=int,
        default=10)
    argsParser.add_argument(
        '--numRequests',
        help='The number of instances per request',
        type=int,
        default=5)                       
    return argsParser.parse_args()
# }} End fnParseArguments()



def logResult(request_id, instances, predictions):
  print('Response from request #{}:'.format(request_id))
  for i in range(len(instances)):
    logger.info('Instance: {}'.format(instances[i]))  
    logger.info('Prediction churn: {}'.format(predictions[i][0]))


def generateSingleInstance():
  
  instance = [
      #gender 
      struct_pb2.Value(string_value=random.choice(["Male","Female"])),
      #senior_citizen
      struct_pb2.Value(number_value=random.choice([0,1])),
      #partner
      struct_pb2.Value(string_value=random.choice(["true","false"])),
      #dependents
       struct_pb2.Value(string_value=random.choice(["true","false"])),
      #phone_service
       struct_pb2.Value(string_value=random.choice(["true","false"])),
      #multiple_lines
       struct_pb2.Value(string_value=random.choice(["true","false"])),
      #internet_service
       struct_pb2.Value(string_value=random.choice(["No","DSL","Fiber optic"])),   
      #online_security
       struct_pb2.Value(string_value=random.choice(["true","false"])),
      #online_backup
       struct_pb2.Value(string_value=random.choice(["true","false"])),
      #device_protection
       struct_pb2.Value(string_value=random.choice(["true","false"])),
      #tech_support
       struct_pb2.Value(string_value=random.choice(["true","false"])),
      #streaming_tv
       struct_pb2.Value(string_value=random.choice(["true","false"])),
      #streaming_movies
       struct_pb2.Value(string_value=random.choice(["true","false"])),
      #contract
       struct_pb2.Value(string_value=random.choice(["One year","Two year","Month-to-month"])),
      #paperless_billing
       struct_pb2.Value(string_value=random.choice(["true","false"])),
      #payment method
       struct_pb2.Value(string_value=random.choice(["Electronic check","Mailed check","Credit card (automatic)","Bank transfer (automatic)"])),
      #monthly_charges
      struct_pb2.Value(number_value=random.uniform(0.0, 200.0)),
      #total_charges
       struct_pb2.Value(number_value=random.uniform(0.0, 10000.0)),
      #churn
       struct_pb2.Value(string_value=random.choice(["true","false"])),
    ]
  return instance


    
async def predict(request_id, client, endpoint, num_instances):
 
  instances = [generateSingleInstance() for _ in range(num_instances)]
  request = PredictRequest(endpoint=endpoint)
  request.instances.extend(instances)
  response = await client.predict(request=request)
  logResult(request_id, instances, response.predictions)


async def fnMain(logger,args):
  """Main function."""
  api_endpoint = f'{args.location}-aiplatform.googleapis.com'
  client_options = ClientOptions(api_endpoint=api_endpoint)
  client = aiplatform_v1.PredictionServiceAsyncClient(client_options=client_options)
  endpoint = 'projects/{}/locations/{}/endpoints/{}'.format(args.projectID,args.location,args.endpointID) 
  logger.info('...Sending {} asynchronous prediction requests with {} instances per request '.format(args.numRequests,args.numInstances))
  await asyncio.gather(*[predict(i+1, client, endpoint, args.numInstances)
                         for i in range(args.numRequests)])

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
# {{ Entry point
    arguments = fnParseArguments()
    logger = fnConfigureLogger()
    asyncio.run(fnMain(logger, arguments))

# }} End enty point  