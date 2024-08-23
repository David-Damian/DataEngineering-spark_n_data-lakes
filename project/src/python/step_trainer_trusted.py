import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node step_trainer_landing
step_trainer_landing_node1724374913409 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1724374913409")

# Script generated for node customer_curated
customer_curated_node1724374913622 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_curated", transformation_ctx="customer_curated_node1724374913622")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from step_trainer_landing t1
JOIN customer_curated t2 
ON t1.serialnumber = t2.serialnumber
'''
SQLQuery_node1724374936529 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":customer_curated_node1724374913622, "step_trainer_landing":step_trainer_landing_node1724374913409}, transformation_ctx = "SQLQuery_node1724374936529")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1724375048809 = glueContext.getSink(path="s3://stedi-datalake/step-trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1724375048809")
step_trainer_trusted_node1724375048809.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1724375048809.setFormat("json")
step_trainer_trusted_node1724375048809.writeFrame(SQLQuery_node1724374936529)
job.commit()
