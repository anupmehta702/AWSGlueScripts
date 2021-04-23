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
    oneFileTransform.write.csv("s3://billing-account-glue-output/transaction/joinFeed/"+now+"/")
    
    
    dailyCustomerAccountBalance = DynamicFrame.fromDF(dfcCustom, glueContext, "dailyCustomerAccountBalance")
    return (DynamicFrameCollection({"CustomTransform2": dailyCustomerAccountBalance}, glueContext))
     
def MyFileBeautificationTransform(glueContext, dfc) -> DynamicFrameCollection:
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import when,col 
    from datetime import date
    from datetime import datetime
    
    dfcCustom = dfc.select(list(dfc.keys())[0]).toDF()
    
    
    #Create DF with headers
    spark = SparkSession.builder.appName('TransactionFeed').getOrCreate()
    simpleData = [("accountId","sortKey","chargeid","transactionid","charge_amount","snapshotSortkey","snapshotTransactionId","status")]
    columns= ["accountId","sortKey","chargeid","transactionid","charge_amount","snapshotSortkey","snapshotTransactionId","status"]
    df = spark.createDataFrame(data = simpleData, schema = columns)
    
    
    #Create union
    unionDF = df.union(dfcCustom)
    unionDF.show()
    
    # Write to file with dd/mm/YY H:M:S
    now = datetime.now().strftime("%d%m%Y%H:%M:%S")
    oneFileTransform = unionDF.repartition(1)
    oneFileTransform.write.csv("s3://billing-account-glue-output/transaction/joinFeed/feedWithHeader/"+now+"/")
    
    
    dailyTransactionFeedWithHeader = DynamicFrame.fromDF(unionDF, glueContext, "dailyTransactionFeedWithHeader")
    
    return (DynamicFrameCollection({"CustomTransform3": dailyTransactionFeedWithHeader}, glueContext))

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
## @type: ApplyMapping
## @args: [mappings = [("accountid", "string", "accountid", "string"), ("sortkey", "string", "sortkey", "string"), ("chargeid", "string", "chargeid", "string"), ("transactionid", "string", "transactionid", "string"), ("charge_amount", "string", "charge_amount", "string")], transformation_ctx = "Transform4"]
## @return: Transform4
## @inputs: [frame = DataSource0]
Transform4 = ApplyMapping.apply(frame = DataSource0, mappings = [("accountid", "string", "accountid", "string"), ("sortkey", "string", "sortkey", "string"), ("chargeid", "string", "chargeid", "string"), ("transactionid", "string", "transactionid", "string"), ("charge_amount", "string", "charge_amount", "string")], transformation_ctx = "Transform4")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"Transform4": Transform4}, glueContext), className = MyTransform, transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [dfc = Transform4]
Transform0 = MyTransform(glueContext, DynamicFrameCollection({"Transform4": Transform4}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform0.keys())[0], transformation_ctx = "Transform9"]
## @return: Transform9
## @inputs: [dfc = Transform0]
Transform9 = SelectFromCollection.apply(dfc = Transform0, key = list(Transform0.keys())[0], transformation_ctx = "Transform9")
## @type: DataSource
## @args: [database = "billingaccountdb", table_name = "ver3_customeraccountbalance", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "billingaccountdb", table_name = "ver3_customeraccountbalance", transformation_ctx = "DataSource1")
## @type: ApplyMapping
## @args: [mappings = [("sortkey", "string", "snapshotSortkey", "string"), ("transactionid", "string", "(right) transactionid", "string"), ("status", "string", "(right) status", "string")], transformation_ctx = "Transform8"]
## @return: Transform8
## @inputs: [frame = DataSource1]
Transform8 = ApplyMapping.apply(frame = DataSource1, mappings = [("sortkey", "string", "snapshotSortkey", "string"), ("transactionid", "string", "(right) transactionid", "string"), ("status", "string", "(right) status", "string")], transformation_ctx = "Transform8")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"Transform8": Transform8}, glueContext), className = MySnapshotTransform, transformation_ctx = "Transform5"]
## @return: Transform5
## @inputs: [dfc = Transform8]
Transform5 = MySnapshotTransform(glueContext, DynamicFrameCollection({"Transform8": Transform8}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform5.keys())[0], transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [dfc = Transform5]
Transform2 = SelectFromCollection.apply(dfc = Transform5, key = list(Transform5.keys())[0], transformation_ctx = "Transform2")
## @type: Join
## @args: [columnConditions = ["="], joinType = right, keys2 = ["(right) transactionid"], keys1 = ["transactionid"], transformation_ctx = "Transform7"]
## @return: Transform7
## @inputs: [frame1 = Transform9, frame2 = Transform2]
Transform9DF = Transform9.toDF()
Transform2DF = Transform2.toDF()
Transform7 = DynamicFrame.fromDF(Transform9DF.join(Transform2DF, (Transform9DF['transactionid'] == Transform2DF['(right) transactionid']), "right"), glueContext, "Transform7")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"Transform7": Transform7}, glueContext), className = MyJoinTransform, transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [dfc = Transform7]
Transform3 = MyJoinTransform(glueContext, DynamicFrameCollection({"Transform7": Transform7}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform3.keys())[0], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [dfc = Transform3]
Transform1 = SelectFromCollection.apply(dfc = Transform3, key = list(Transform3.keys())[0], transformation_ctx = "Transform1")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"Transform1": Transform1}, glueContext), className = MyFileBeautificationTransform, transformation_ctx = "Transform6"]
## @return: Transform6
## @inputs: [dfc = Transform1]
Transform6 = MyFileBeautificationTransform(glueContext, DynamicFrameCollection({"Transform1": Transform1}, glueContext))
job.commit()
