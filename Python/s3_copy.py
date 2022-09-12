#!/usr/bin/python

import boto3
from datetime import datetime

# current date and time
now = datetime.now() 

# Define the input file name and the output file name
date_time = now.strftime("%Y-%b-%d_%H.%M.%S")
output_filename = 'destination_data/output_data_file.csv.' + date_time;
input_filename = 'source_data/input_data_file.csv'

#Creating Session With Boto3.
session = boto3.Session();

#Creating S3 Resource From the Session.
s3 = session.resource('s3')

#Create a Source tuple That Specifies Bucket Name and Key Name of the Object to Be Copied
copy_source = {
    'Bucket': 'raghu-pushpakath-poc-bucket-oregon',
    'Key': input_filename
}

# Specify the target bucket and copy the file to the destination
bucket = s3.Bucket('raghu-pushpakath-poc-bucket-oregon')
bucket.copy(copy_source, output_filename)

# Printing the Information That the File Is Copied.
print('Copied the file ' + input_filename + ' to ' + output_filename)