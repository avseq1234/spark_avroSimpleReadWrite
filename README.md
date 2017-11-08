# spark_avroSimpleReadWrite
This is a simple code to read and write avro file in Spark

df.write.partitionBy("year","month").avro("avrotest123")

The forlder in HDFS will be as following

avrotest123/year=2012/month=7

avrotest123/year=2012/month=8

avrotest123/year=2011/month=7
