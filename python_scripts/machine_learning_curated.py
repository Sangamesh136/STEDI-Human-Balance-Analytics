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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1745309234174 = glueContext.create_dynamic_frame.from_catalog(database="udacity-stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1745309234174")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1745309262256 = glueContext.create_dynamic_frame.from_catalog(database="udacity-stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1745309262256")

# Script generated for node SQL Query
SqlQuery650 = '''
select distinct step_trainer_trusted.sensorreadingtime,
step_trainer_trusted.serialnumber,
step_trainer_trusted.distancefromobject,
accelerometer_trusted.user,
accelerometer_trusted.x,
accelerometer_trusted.y,
accelerometer_trusted.z
from step_trainer_trusted
inner join accelerometer_trusted
on accelerometer_trusted.timestamp =
step_trainer_trusted.sensorreadingtime;
'''
SQLQuery_node1745309468907 = sparkSqlQuery(glueContext, query = SqlQuery650, mapping = {"step_trainer_trusted":step_trainer_trusted_node1745309234174, "accelerometer_trusted":accelerometer_trusted_node1745309262256}, transformation_ctx = "SQLQuery_node1745309468907")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745309468907, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745309193846", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745309985957 = glueContext.getSink(path="s3://udacity-data-lake/curated/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745309985957")
AmazonS3_node1745309985957.setCatalogInfo(catalogDatabase="udacity-stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1745309985957.setFormat("json")
AmazonS3_node1745309985957.writeFrame(SQLQuery_node1745309468907)
job.commit()