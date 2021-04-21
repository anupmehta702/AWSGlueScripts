import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    dfcCustom = dfc.select(list(dfc.keys())[0]).toDF()
    
    from pyspark.sql.functions import when 
    
    dfcCustom = dfcCustom.withColumn('daily_updated_balance',
    when(dfcCustom.daily_balance.isNull() ,dfcCustom.current_balance)
    .otherwise(dfcCustom.daily_balance))
    dfcCustom = dfcCustom.drop('(right) ppeaccountid')
    dfcCustom.show();
       
    oneFileTransform = dfcCustom.repartition(1)
    oneFileTransform.write.json("s3://billing-account-glue-output/singleFileOutput3/")
    
    
    dailyCustomerAccountBalance = DynamicFrame.fromDF(dfcCustom, glueContext, "dailyCustomerAccountBalance")
    return (DynamicFrameCollection({"CustomTransform0": dailyCustomerAccountBalance}, glueContext))

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "billingaccountdb", table_name = "customeraccountbalance_customeraccountbalance", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "billingaccountdb", table_name = "customeraccountbalance_customeraccountbalance", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("balance", "string", "daily_balance", "string"), ("ppeaccountid", "string", "(right) ppeaccountid", "string")], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame = DataSource0]
Transform1 = ApplyMapping.apply(frame = DataSource0, mappings = [("balance", "string", "daily_balance", "string"), ("ppeaccountid", "string", "(right) ppeaccountid", "string")], transformation_ctx = "Transform1")
## @type: DataSource
## @args: [database = "billingaccountdb", table_name = "ver2_billingaccount", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "billingaccountdb", table_name = "ver2_billingaccount", transformation_ctx = "DataSource1")
## @type: ApplyMapping
## @args: [mappings = [("balance", "string", "current_balance", "string"), ("ppeaccountid", "string", "ppeaccountid", "string"), ("status", "string", "status", "string")], transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [frame = DataSource1]
Transform2 = ApplyMapping.apply(frame = DataSource1, mappings = [("balance", "string", "current_balance", "string"), ("ppeaccountid", "string", "ppeaccountid", "string"), ("status", "string", "status", "string")], transformation_ctx = "Transform2")
## @type: Join
## @args: [columnConditions = ["="], joinType = left, keys2 = ["(right) ppeaccountid"], keys1 = ["ppeaccountid"], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame1 = Transform2, frame2 = Transform1]
Transform2DF = Transform2.toDF()
Transform1DF = Transform1.toDF()
Transform0 = DynamicFrame.fromDF(Transform2DF.join(Transform1DF, (Transform2DF['ppeaccountid'] == Transform1DF['(right) ppeaccountid']), "left"), glueContext, "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://billing-account-glue-output/S3Output/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "json", connection_options = {"path": "s3://billing-account-glue-output/S3Output/", "partitionKeys": []}, transformation_ctx = "DataSink0")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"Transform0": Transform0}, glueContext), className = MyTransform, transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [dfc = Transform0]
Transform3 = MyTransform(glueContext, DynamicFrameCollection({"Transform0": Transform0}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform3.keys())[0], transformation_ctx = "Transform4"]
## @return: Transform4
## @inputs: [dfc = Transform3]
Transform4 = SelectFromCollection.apply(dfc = Transform3, key = list(Transform3.keys())[0], transformation_ctx = "Transform4")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://billing-account-glue-output/spigot/", "partitionKeys": []}, transformation_ctx = "DataSink1"]
## @return: DataSink1
## @inputs: [frame = Transform4]
DataSink1 = glueContext.write_dynamic_frame.from_options(frame = Transform4, connection_type = "s3", format = "json", connection_options = {"path": "s3://billing-account-glue-output/spigot/", "partitionKeys": []}, transformation_ctx = "DataSink1")
job.commit()
