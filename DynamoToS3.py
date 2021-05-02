import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

def PrintBillingAcctData(glueContext, dfc) -> DynamicFrameCollection:
    
    from pyspark.sql.functions import when 
    from datetime import date
    from datetime import datetime
    
    dfcCustom = dfc.select(list(dfc.keys())[0]).toDF()
    print("Printing data from S3 export of Billing Account --")
    dfcCustom.show();
    
    # dd/mm/YY H:M:S
    now = datetime.now().strftime("%d%m%Y")
    previousBillingAcct = glueContext.create_dynamic_frame.from_options(format_options = {"jsonPath":"","multiline":False}, connection_type = "s3", format = "json", connection_options = {"paths": ["s3://billing-account-s3-backup/manual/"+now+"/"], "recurse":True}, transformation_ctx = "DataSource1").toDF()
    print("Printig previousBillingAcct -->")
    previousBillingAcct.show()
    
    dailyCustomerAccountBalance = DynamicFrame.fromDF(dfcCustom, glueContext, "dailyCustomerAccountBalance")
    return (DynamicFrameCollection({"CustomTransform0": dailyCustomerAccountBalance}, glueContext))
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    
    from pyspark.sql.functions import when 
    from datetime import date
    from datetime import datetime
    
    dfcCustom = dfc.select(list(dfc.keys())[0]).toDF()
    
    dfcCustom = dfcCustom.withColumn('daily_updated_balance',
    when(dfcCustom.daily_balance.isNull() ,dfcCustom.balance)
    .otherwise(dfcCustom.daily_balance))
    dfcCustom = dfcCustom.drop('(right) ppeaccountid')
    print("Printing dataframe with daily updated balance field -->")
    dfcCustom.show();
    
    # dd/mm/YY H:M:S
    now = datetime.now().strftime("%d%m%Y%H:%M:%S")
    
    oneFileTransform = dfcCustom.repartition(1)
    oneFileTransform.write.json("s3://billing-account-glue-output/billing-account/join/"+now+"/")
    
    
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
## @args: [database = "billingaccountdb", table_name = "ver3_customeraccountbalance", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "billingaccountdb", table_name = "ver3_customeraccountbalance", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("transactionid", "string", "transactionid", "string"), ("ppeaccountid", "string", "(right) ppeaccountid", "string"), ("balance", "string", "daily_balance", "string")], transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [frame = DataSource0]
Transform2 = ApplyMapping.apply(frame = DataSource0, mappings = [("transactionid", "string", "transactionid", "string"), ("ppeaccountid", "string", "(right) ppeaccountid", "string"), ("balance", "string", "daily_balance", "string")], transformation_ctx = "Transform2")
## @type: DataSource
## @args: [format_options = {"jsonPath":"","multiline":False}, connection_type = "s3", format = "json", connection_options = {"paths": ["s3://billing-account-s3-backup/manual/"], "recurse":True}, transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_options(format_options = {"jsonPath":"","multiline":False}, connection_type = "s3", format = "json", connection_options = {"paths": ["s3://billing-account-s3-backup/manual/"], "recurse":True}, transformation_ctx = "DataSource1")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"DataSource1": DataSource1}, glueContext), className = PrintBillingAcctData, transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [dfc = DataSource1]
Transform3 = PrintBillingAcctData(glueContext, DynamicFrameCollection({"DataSource1": DataSource1}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform3.keys())[0], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [dfc = Transform3]
Transform1 = SelectFromCollection.apply(dfc = Transform3, key = list(Transform3.keys())[0], transformation_ctx = "Transform1")
## @type: ApplyMapping
## @args: [mappings = [("status", "string", "status", "string"), ("partitionKey", "string", "partitionKey", "string"), ("balance", "string", "balance", "string"), ("ppeAccountId", "string", "ppeAccountId", "string"), ("sortKey", "string", "sortKey", "string")], transformation_ctx = "Transform4"]
## @return: Transform4
## @inputs: [frame = Transform1]
Transform4 = ApplyMapping.apply(frame = Transform1, mappings = [("status", "string", "status", "string"), ("partitionKey", "string", "partitionKey", "string"), ("balance", "string", "balance", "string"), ("ppeAccountId", "string", "ppeAccountId", "string"), ("sortKey", "string", "sortKey", "string")], transformation_ctx = "Transform4")
## @type: Join
## @args: [columnConditions = ["="], joinType = left, keys2 = ["(right) ppeaccountid"], keys1 = ["ppeAccountId"], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame1 = Transform4, frame2 = Transform2]
Transform4DF = Transform4.toDF()
Transform2DF = Transform2.toDF()
Transform0 = DynamicFrame.fromDF(Transform4DF.join(Transform2DF, (Transform4DF['ppeAccountId'] == Transform2DF['(right) ppeaccountid']), "left"), glueContext, "Transform0")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"Transform0": Transform0}, glueContext), className = MyTransform, transformation_ctx = "Transform5"]
## @return: Transform5
## @inputs: [dfc = Transform0]
Transform5 = MyTransform(glueContext, DynamicFrameCollection({"Transform0": Transform0}, glueContext))
job.commit()
