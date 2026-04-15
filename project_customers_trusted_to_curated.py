import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1776209774251 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1776209774251")

# Script generated for node Customer Trusted
CustomerTrusted_node1776209772789 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1776209772789")

# Script generated for node Join
Join_node1776209813319 = Join.apply(frame1=AccelerometerTrusted_node1776209774251, frame2=CustomerTrusted_node1776209772789, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1776209813319")

# Script generated for node Drop Fields
SqlQuery1655 = '''
select serialnumber, sharewithpublicasofdate,
birthday, registrationdate, sharewithresearchasofdate,
customername, email, lastupdatedate, phone, sharewithfriendsasofdate
from myDataSource
'''
DropFields_node1776211105658 = sparkSqlQuery(glueContext, query = SqlQuery1655, mapping = {"myDataSource":Join_node1776209813319}, transformation_ctx = "DropFields_node1776211105658")

# Script generated for node Drop Duplicates
DropDuplicates_node1776211915960 =  DynamicFrame.fromDF(DropFields_node1776211105658.toDF().dropDuplicates(["serialnumber"]), glueContext, "DropDuplicates_node1776211915960")

# Script generated for node Customers Curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1776211915960, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776199253396", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomersCurated_node1776209833897 = glueContext.getSink(path="s3://udacity-landing-zone-bucket/customers_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomersCurated_node1776209833897")
CustomersCurated_node1776209833897.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customers_curated")
CustomersCurated_node1776209833897.setFormat("json")
CustomersCurated_node1776209833897.writeFrame(DropDuplicates_node1776211915960)
job.commit()