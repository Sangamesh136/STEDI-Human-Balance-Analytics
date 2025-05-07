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

# Script generated for node Amazon S3
AmazonS3_node1745214359788 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-data-lake/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1745214359788")

# Script generated for node SQL Query
SqlQuery838 = '''
select * from myDataSource
where sharewithresearchasofdate is not null;
'''
SQLQuery_node1745239590547 = sparkSqlQuery(glueContext, query = SqlQuery838, mapping = {"myDataSource":AmazonS3_node1745214359788}, transformation_ctx = "SQLQuery_node1745239590547")

# Script generated for node Trusted Customer Zone
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745239590547, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745214227531", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
TrustedCustomerZone_node1745215302903 = glueContext.getSink(path="s3://udacity-data-lake/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrustedCustomerZone_node1745215302903")
TrustedCustomerZone_node1745215302903.setCatalogInfo(catalogDatabase="udacity-stedi",catalogTableName="customer_trusted")
TrustedCustomerZone_node1745215302903.setFormat("json")
TrustedCustomerZone_node1745215302903.writeFrame(SQLQuery_node1745239590547)
job.commit()