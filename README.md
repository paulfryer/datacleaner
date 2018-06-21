# datacleaner
A serverless application for automatically cleaning data placed in an S3 bucket and delivering it to a second "clean" bucket. This is a foundation pattern for implementing your own data cleaning patterns, typically for compliance reasons.

To set up this project first clone it then deploy the SAM template.

Next copy the example SampleData.csv file in the SampleData folder to your "raw" S3 bucket. This bucket (and a clean one) where created when your deployed your SAM template. Copy the file into a fold named "PII" or any other name.

Execute the "Crawler" step function. That will kick off the glue crawler which will look at all the data in the raw bucket and create tables in the glue catalog.

Once it has all the tables indexed it will kick of individual "Clean" step functions for each table. This will copy all the data from the raw table, run it through a "clean" function and deliver the cleaned values to the "Clean" bucket via Kinesis. 

