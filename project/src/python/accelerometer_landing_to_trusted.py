import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer trusted
Customertrusted_node1724356045323 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalake/customer/trusted/"], "recurse": True}, transformation_ctx="Customertrusted_node1724356045323")

# Script generated for node Accelerometer landing
Accelerometerlanding_node1724356046303 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalake/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometerlanding_node1724356046303")

# Script generated for node Join
Join_node1724356078360 = Join.apply(frame1=Accelerometerlanding_node1724356046303, frame2=Customertrusted_node1724356045323, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1724356078360")

# Script generated for node Drop Fields
DropFields_node1724356088586 = DropFields.apply(frame=Join_node1724356078360, paths=["customerName", "email", "phone", "birthDay", "serialNumber", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"], transformation_ctx="DropFields_node1724356088586")

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1724356101647 = glueContext.getSink(path="s3://stedi-datalake/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometertrusted_node1724356101647")
Accelerometertrusted_node1724356101647.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="accelerometer_trusted")
Accelerometertrusted_node1724356101647.setFormat("json")
Accelerometertrusted_node1724356101647.writeFrame(DropFields_node1724356088586)
job.commit()
