"""
This script processes step trainer data by removing non-consenting customers on research. 

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

# Script generated for node Customer Curated
CustomerCurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sangwon-stedi/customer/curated_distinct/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1676072957579 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sangwon-stedi/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1676072957579",
)

# Script generated for node Customer Curated Filter
CustomerCuratedFilter_node2 = Join.apply(
    frame1=CustomerCurated_node1,
    frame2=StepTrainerLanding_node1676072957579,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="CustomerCuratedFilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1676073014820 = DropFields.apply(
    frame=CustomerCuratedFilter_node2,
    paths=[
        "email",
        "phone",
        "lastUpdateDate",
        "shareWithPublicAsOfDate",
        "birthDay",
        "timeStamp",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1676073014820",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1676073128583 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1676073014820,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://sangwon-stedi/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1676073128583",
)

job.commit()
