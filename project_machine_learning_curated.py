import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1776213205751 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1776213205751")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1776213206651 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1776213206651")

# Script generated for node Join
Join_node1776213248741 = Join.apply(frame1=AccelerometerTrusted_node1776213206651, frame2=StepTrainerTrusted_node1776213205751, keys1=["timestamp"], keys2=["right_sensorreadingtime"], transformation_ctx="Join_node1776213248741")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=Join_node1776213248741, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776212092857", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1776213316332 = glueContext.getSink(path="s3://udacity-landing-zone-bucket/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1776213316332")
MachineLearningCurated_node1776213316332.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1776213316332.setFormat("json")
MachineLearningCurated_node1776213316332.writeFrame(Join_node1776213248741)
job.commit()