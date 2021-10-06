#### [SaprkConf] ####
##spark.master= leave blank for local spark setup
spark.master=local

#spark.appname=your applicatio instance Name
spark.appname=KSV-Spark-JR1

#### [Mongo DB] ####
spark.mongodb.input.uri=mongodb://10.50.8.220:27017/astm_feeder.201711_page_types
spark.mongodb.output.uri=mongodb://10.50.8.220:27017/astm_feeder.201711_page_types

spark.local.dir=/storage/counter5/SPARK_TEMP/


