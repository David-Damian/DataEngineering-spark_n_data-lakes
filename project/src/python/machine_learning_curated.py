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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1724383386253 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1724383386253")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1724383386054 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1724383386054")

# Script generated for node join by timestamp and sensorReadingTime
SqlQuery0 = '''
select customername	
email, x, y, z,
sensorreadingtime,	
distancefromobject,
phone,	
birthday,	
registrationdate,	
lastupdatedate,
sharewithresearchasofdate,
sharewithpublicasofdate,
sharewithfriendsasofdate
from step_trainer_trusted t1
JOIN accelerometer_trusted t2
ON t1.sensorReadingTime = t2.timeStamp
'''
joinbytimestampandsensorReadingTime_node1724383583505 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":step_trainer_trusted_node1724383386054, "accelerometer_trusted":accelerometer_trusted_node1724383386253}, transformation_ctx = "joinbytimestampandsensorReadingTime_node1724383583505")

# Script generated for node machine learning curated
machinelearningcurated_node1724383673029 = glueContext.getSink(path="s3://stedi-datalake/machine-learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="machinelearningcurated_node1724383673029")
machinelearningcurated_node1724383673029.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="machine_learning_curated")
machinelearningcurated_node1724383673029.setFormat("json")
machinelearningcurated_node1724383673029.writeFrame(joinbytimestampandsensorReadingTime_node1724383583505)
job.commit()
