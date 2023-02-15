"""
This script create glue jobs to process customer data in trusted zone.

Author: Sangwon Lee
Date:2023/02/09
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1676072957579 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1676072957579",
)

# Script generated for node Customer Curated Filter
CustomerCuratedFilter_node2 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerTrusted_node1676072957579,
    keys1=["customername"],
    keys2=["user"],
    transformation_ctx="CustomerCuratedFilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1676073014820 = DropFields.apply(
    frame=CustomerCuratedFilter_node2,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1676073014820",
)

# Script generated for node Drop Duplicate
DropDuplicate_node1676102161958 = DynamicFrame.fromDF(
    DropFields_node1676073014820.toDF().dropDuplicates(["email", "serialnumber"]),
    glueContext,
    "DropDuplicate_node1676102161958",
)

# Script generated for node Customer Curated
CustomerCurated_node1676073128583 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicate_node1676102161958,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://sangwon-stedi/customer/Curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1676073128583",
)

job.commit()
