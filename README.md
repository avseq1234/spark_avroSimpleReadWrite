# spark_avroSimpleReadWrite
This is a simple code to read and write avro file in Spark<br>
df.write.partitionBy("year","month").avro("avrotest123")<br>
The forlder in HDFS will be as following<br>
avrotest123/year=2012/month=7<br>
avrotest123/year=2012/month=8<br>
avrotest123/year=2011/month=7<br>
