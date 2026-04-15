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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1776212453838 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1776212453838")

# Script generated for node Customers Curated
CustomersCurated_node1776212455575 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customers_curated", transformation_ctx="CustomersCurated_node1776212455575")

# Script generated for node Renamed fields for Join
RenamedfieldsforJoin_node1776212550548 = ApplyMapping.apply(frame=StepTrainerLanding_node1776212453838, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "long", "right_distancefromobject", "long")], transformation_ctx="RenamedfieldsforJoin_node1776212550548")

# Script generated for node Join
Join_node1776212503557 = Join.apply(frame1=CustomersCurated_node1776212455575, frame2=RenamedfieldsforJoin_node1776212550548, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1776212503557")

# Script generated for node Drop Fields
SqlQuery1745 = '''
select right_sensorreadingtime, right_distancefromobject,
right_serialnumber
from myDataSource

'''
DropFields_node1776212628311 = sparkSqlQuery(glueContext, query = SqlQuery1745, mapping = {"myDataSource":Join_node1776212503557}, transformation_ctx = "DropFields_node1776212628311")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1776212628311, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776212092857", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1776212727586 = glueContext.getSink(path="s3://udacity-landing-zone-bucket/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1776212727586")
StepTrainerTrusted_node1776212727586.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1776212727586.setFormat("json")
StepTrainerTrusted_node1776212727586.writeFrame(DropFields_node1776212628311)
job.commit()