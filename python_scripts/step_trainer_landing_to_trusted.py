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

# Script generated for node step_trainer_landing
step_trainer_landing_node1745469227467 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-data-lake/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1745469227467")

# Script generated for node customer_curated
customer_curated_node1745469224996 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-data-lake/customer/curated"], "recurse": True}, transformation_ctx="customer_curated_node1745469224996")

# Script generated for node SQL Query
SqlQuery173 = '''
select step_trainer_landing.* 
from step_trainer_landing 
inner join customer_curated
on step_trainer_landing.serialnumber = customer_curated.serialnumber
'''
SQLQuery_node1745305488497 = sparkSqlQuery(glueContext, query = SqlQuery173, mapping = {"customer_curated":customer_curated_node1745469224996, "step_trainer_landing":step_trainer_landing_node1745469227467}, transformation_ctx = "SQLQuery_node1745305488497")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745305488497, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745304670007", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1745305167323 = glueContext.getSink(path="s3://udacity-data-lake/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1745305167323")
step_trainer_trusted_node1745305167323.setCatalogInfo(catalogDatabase="udacity-stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1745305167323.setFormat("json")
step_trainer_trusted_node1745305167323.writeFrame(SQLQuery_node1745305488497)
job.commit()