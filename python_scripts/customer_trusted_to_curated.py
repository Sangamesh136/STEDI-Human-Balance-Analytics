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
customer_trusted_node1745303063082 = glueContext.create_dynamic_frame.from_catalog(database="udacity-stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1745303063082")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1745303089390 = glueContext.create_dynamic_frame.from_catalog(database="udacity-stedi", table_name="accelerometer_trusted1", transformation_ctx="accelerometer_trusted_node1745303089390")

# Script generated for node Join
Join_node1745303116351 = Join.apply(frame1=accelerometer_trusted_node1745303089390, frame2=customer_trusted_node1745303063082, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1745303116351")

# Script generated for node SQL Query
SqlQuery259 = '''
select distinct customername, email, phone, birthday, 
serialnumber, registrationdate, lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource

'''
SQLQuery_node1745303149422 = sparkSqlQuery(glueContext, query = SqlQuery259, mapping = {"myDataSource":Join_node1745303116351}, transformation_ctx = "SQLQuery_node1745303149422")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745303149422, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745303055365", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1745303246382 = glueContext.getSink(path="s3://udacity-data-lake/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1745303246382")
customer_curated_node1745303246382.setCatalogInfo(catalogDatabase="udacity-stedi",catalogTableName="customer_curated")
customer_curated_node1745303246382.setFormat("json")
customer_curated_node1745303246382.writeFrame(SQLQuery_node1745303149422)
job.commit()