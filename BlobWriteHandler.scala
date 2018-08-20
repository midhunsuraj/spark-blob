package com.probi.spark.rss

import org.apache.spark.sql.{SaveMode, SparkSession}

object BlobWriteHandler {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]").appName("Blob Spark Application").getOrCreate()


    spark.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    spark.conf.set("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
    spark.conf.set("fs.azure.account.key.datadiskone.blob.core.windows.net","6jxy2+bySXcWqpe+g2/Z/w9G8YBfvVQt6zw6Uc6yene3zCwttSlCXKvbixcE3/81vxFGJwZffkNw+gWRizsqXA==")



    //file:/E:/data/sample.csv
    val df = spark.read.format("csv").option("header","true").load("file:/E:/data/sample.csv")

    df.createOrReplaceTempView("blobdata")

    val outputdt = spark.sql("select id,name,gender from blobdata").coalesce(1)

    outputdt.show()

    val url = "wasbs://local-data@datadiskone.blob.core.windows.net/rssfeed"
    outputdt.write.csv(url)
    spark.close()



  }


}
