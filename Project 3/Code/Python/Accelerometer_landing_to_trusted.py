"""
This script create glue jobs to process Accelerometer data on landing zone. Gluejobs remove any customers who have not given any consent to share their data for research.  

Author: Sangwon Lee
Date:2023/02/09

"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer_Trusted
Customer_Trusted_node1676025082004 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="Customer_Trusted_node1676025082004",
)

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sangwon-stedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Landing_node1",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node2 = Join.apply(
    frame1=Accelerometer_Landing_node1,
    frame2=Customer_Trusted_node1676025082004,
    keys1=["user"],
    keys2=["customername"],
    transformation_ctx="CustomerPrivacyFilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1676025346385 = DropFields.apply(
    frame=CustomerPrivacyFilter_node2,
    paths=[
        "customername",
        "email",
        "phone",
        "birthdate",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1676025346385",
)

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1676025346385,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://sangwon-stedi/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Accelerometer_Trusted_node3",
)

job.commit()
