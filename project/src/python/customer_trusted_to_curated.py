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

# Script generated for node customer landing
customerlanding_node1724372229312 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalake/customer/landing/"], "recurse": True}, transformation_ctx="customerlanding_node1724372229312")

# Script generated for node accelerometer trusted
accelerometertrusted_node1724372229491 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalake/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometertrusted_node1724372229491")

# Script generated for node Join
Join_node1724372300086 = Join.apply(frame1=customerlanding_node1724372229312, frame2=accelerometertrusted_node1724372229491, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1724372300086")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct customername, email, phone, birthday, 
serialnumber, registrationdate, lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate FROM myDataSource

'''
SQLQuery_node1724372320390 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1724372300086}, transformation_ctx = "SQLQuery_node1724372320390")

# Script generated for node customer curated
customercurated_node1724372335569 = glueContext.getSink(path="s3://stedi-datalake/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customercurated_node1724372335569")
customercurated_node1724372335569.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_curated")
customercurated_node1724372335569.setFormat("json")
customercurated_node1724372335569.writeFrame(SQLQuery_node1724372320390)
job.commit()
