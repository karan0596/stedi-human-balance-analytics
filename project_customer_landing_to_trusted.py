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

# Script generated for node Customer Landing
CustomerLanding_node1776192785131 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_landing", transformation_ctx="CustomerLanding_node1776192785131")

# Script generated for node PrivacyFilter
SqlQuery1771 = '''
select * from myDataSource
where sharewithresearchasofdate is not NULL;
'''
PrivacyFilter_node1776197763590 = sparkSqlQuery(glueContext, query = SqlQuery1771, mapping = {"myDataSource":CustomerLanding_node1776192785131}, transformation_ctx = "PrivacyFilter_node1776197763590")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=PrivacyFilter_node1776197763590, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776191330282", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1776192341841 = glueContext.getSink(path="s3://udacity-landing-zone-bucket/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1776192341841")
CustomerTrusted_node1776192341841.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
CustomerTrusted_node1776192341841.setFormat("json")
CustomerTrusted_node1776192341841.writeFrame(PrivacyFilter_node1776197763590)
job.commit()