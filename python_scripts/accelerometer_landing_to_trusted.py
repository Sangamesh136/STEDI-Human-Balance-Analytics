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

# Script generated for node customer_trusted
customer_trusted_node1745468966147 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-data-lake/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1745468966147")

# Script generated for node accelerometer_landing
accelerometer_landing_node1745468828353 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-data-lake/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1745468828353")

# Script generated for node Join
Join_node1745313665438 = Join.apply(frame1=accelerometer_landing_node1745468828353, frame2=customer_trusted_node1745468966147, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1745313665438")

# Script generated for node SQL Query
SqlQuery258 = '''
select user,timestamp, x, y, z from myDataSource;
'''
SQLQuery_node1745313731723 = sparkSqlQuery(glueContext, query = SqlQuery258, mapping = {"myDataSource":Join_node1745313665438}, transformation_ctx = "SQLQuery_node1745313731723")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745313731723, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745313562846", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745313803456 = glueContext.getSink(path="s3://udacity-data-lake/accelerometer/trusted1/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745313803456")
AmazonS3_node1745313803456.setCatalogInfo(catalogDatabase="udacity-stedi",catalogTableName="accelerometer_trusted1")
AmazonS3_node1745313803456.setFormat("json")
AmazonS3_node1745313803456.writeFrame(SQLQuery_node1745313731723)
job.commit()