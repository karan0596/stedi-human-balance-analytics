import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

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

# Script generated for node Customer Trusted
CustomerTrusted_node1776198056741 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1776198056741")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1776198056070 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1776198056070")

# Script generated for node Join
Join_node1776198085862 = Join.apply(frame1=CustomerTrusted_node1776198056741, frame2=AccelerometerLanding_node1776198056070, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1776198085862")

# Script generated for node Drop Fields
SqlQuery1416 = '''
select user, timestamp, x, y, z 
from myDataSource;

'''
DropFields_node1776199276722 = sparkSqlQuery(glueContext, query = SqlQuery1416, mapping = {"myDataSource":Join_node1776198085862}, transformation_ctx = "DropFields_node1776199276722")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1776199276722, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776198048325", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1776198251393 = glueContext.getSink(path="s3://udacity-landing-zone-bucket/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1776198251393")
AccelerometerTrusted_node1776198251393.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1776198251393.setFormat("json")
AccelerometerTrusted_node1776198251393.writeFrame(DropFields_node1776199276722)
job.commit()