import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    
    from pyspark.sql.functions import when,col 
    from datetime import date
    from datetime import datetime
    
    dfcCustom = dfc.select(list(dfc.keys())[0]).toDF()
    dfcCustom = dfcCustom.filter(col("sortKey").contains("CHARGE#"))
    dfcCustom.show();
    
    # dd/mm/YY H:M:S
    now = datetime.now().strftime("%d%m%Y%H:%M:%S")
    
    oneFileTransform = dfcCustom.repartition(1)
    oneFileTransform.write.json("s3://billing-account-glue-output/transaction/"+now+"/")
    
    
    dailyCustomerAccountBalance = DynamicFrame.fromDF(dfcCustom, glueContext, "dailyCustomerAccountBalance")
    return (DynamicFrameCollection({"CustomTransform0": dailyCustomerAccountBalance}, glueContext))
     
def MySnapshotTransform(glueContext, dfc) -> DynamicFrameCollection:
    
     
    from pyspark.sql.functions import when,col 
    from datetime import date
    from datetime import datetime
    
    dfcSnapshotCustom = dfc.select(list(dfc.keys())[0]).toDF()
    dfcSnapshotCustom = dfcSnapshotCustom.filter(col("snapshotSortkey").contains("TRANSACTION#"))
    
    # dd/mm/YY H:M:S
    now = datetime.now().strftime("%d%m%Y%H:%M:%S")
    
    oneFileForSnapshotTransform = dfcSnapshotCustom.repartition(1)
    oneFileForSnapshotTransform.write.json(
         "s3://billing-account-glue-output/transaction/filteredSnapshot/"+now+"/")
    
    
    snapshotBalance = DynamicFrame.fromDF(dfcSnapshotCustom, glueContext, "snapshotBalance")
    return (DynamicFrameCollection({"CustomTransform1": snapshotBalance}, glueContext)) 
def MyJoinTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import when 
    from datetime import date
    from datetime import datetime
    
    dfcCustom = dfc.select(list(dfc.keys())[0]).toDF()
    dfcCustom.show();
    
    # dd/mm/YY H:M:S
    now = datetime.now().strftime("%d%m%Y%H:%M:%S")
    
    oneFileTransform = dfcCustom.repartition(1)
    oneFileTransform.write.json("s3://billing-account-glue-output/transaction/joinFeed/"+now+"/")
    
    
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
## @args: [database = "billingaccountdb", table_name = "transaction_transaction", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "billingaccountdb", table_name = "transaction_transaction", transformation_ctx = "DataSource0")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"DataSource0": DataSource0}, glueContext), className = MyTransform, transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [dfc = DataSource0]
Transform0 = MyTransform(glueContext, DynamicFrameCollection({"DataSource0": DataSource0}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform0.keys())[0], transformation_ctx = "Transform6"]
## @return: Transform6
## @inputs: [dfc = Transform0]
Transform6 = SelectFromCollection.apply(dfc = Transform0, key = list(Transform0.keys())[0], transformation_ctx = "Transform6")
## @type: DataSource
## @args: [database = "billingaccountdb", table_name = "ver3_customeraccountbalance", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "billingaccountdb", table_name = "ver3_customeraccountbalance", transformation_ctx = "DataSource1")
## @type: ApplyMapping
## @args: [mappings = [("sortkey", "string", "snapshotSortkey", "string"), ("transactionid", "string", "(right) transactionid", "string"), ("ppeaccountid", "string", "(right) ppeaccountid", "string"), ("status", "string", "(right) status", "string")], transformation_ctx = "Transform5"]
## @return: Transform5
## @inputs: [frame = DataSource1]
Transform5 = ApplyMapping.apply(frame = DataSource1, mappings = [("sortkey", "string", "snapshotSortkey", "string"), ("transactionid", "string", "(right) transactionid", "string"), ("ppeaccountid", "string", "(right) ppeaccountid", "string"), ("status", "string", "(right) status", "string")], transformation_ctx = "Transform5")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"Transform5": Transform5}, glueContext), className = MySnapshotTransform, transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [dfc = Transform5]
Transform3 = MySnapshotTransform(glueContext, DynamicFrameCollection({"Transform5": Transform5}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform3.keys())[0], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [dfc = Transform3]
Transform1 = SelectFromCollection.apply(dfc = Transform3, key = list(Transform3.keys())[0], transformation_ctx = "Transform1")
## @type: Join
## @args: [columnConditions = ["="], joinType = right, keys2 = ["(right) transactionid"], keys1 = ["transactionid"], transformation_ctx = "Transform4"]
## @return: Transform4
## @inputs: [frame1 = Transform6, frame2 = Transform1]
Transform6DF = Transform6.toDF()
Transform1DF = Transform1.toDF()
Transform4 = DynamicFrame.fromDF(Transform6DF.join(Transform1DF, (Transform6DF['transactionid'] == Transform1DF['(right) transactionid']), "right"), glueContext, "Transform4")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"Transform4": Transform4}, glueContext), className = MyJoinTransform, transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [dfc = Transform4]
Transform2 = MyJoinTransform(glueContext, DynamicFrameCollection({"Transform4": Transform4}, glueContext))
job.commit()
