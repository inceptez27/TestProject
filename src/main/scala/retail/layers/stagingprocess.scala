package retail.layers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import retail.driver.rundriver.logger
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions.regexp_replace
import java.util.Properties

object stagingprocess 
{
  
  val format = new SimpleDateFormat("yyyy-MM-dd h:m:s")
  
  def stageprocess(spark:SparkSession,prop:Properties)=
  {
    //create database
    spark.sql("DROP DATABASE IF EXISTS retail_stg CASCADE")
    spark.sql("create database if not exists retail_stg")
    
     if(prop.getProperty("srctype").trim() == "S3")
      {
        println("Into s3")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", prop.getProperty("fs.s3a.awsAccessKeyId"))
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", prop.getProperty("fs.s3a.awsSecretAccessKey"))
        spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider") 
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
      }
    
    logger.warn("======staging process started at " + format.format(Calendar.getInstance().getTime()))
    
    //Read customer data from csv file and load into tblcustomer_stg under database retail_stg
    readfileandwriteintostaging(spark,prop.getProperty("srccustomer"),"retail_stg.tblcustomer_stg")
    
     //Read product category data from csv file and load into tblproductcategory_stg under database retail_stg
    readfileandwriteintostaging(spark,prop.getProperty("srcproductcategory"),"retail_stg.tblproductcategory_stg")
    
    readfileandwriteintostaging(spark,prop.getProperty("srcproductsubcategory"),"retail_stg.tblproductsubcategory_stg")
    
    readfileandwriteintostaging(spark,prop.getProperty("srcsales"),"retail_stg.tblsales_stg")
    
    readfileandwriteintostaging(spark,prop.getProperty("srcterritory"),"retail_stg.tblterritory_stg")
    
    readfileandwriteintostaging(spark,prop.getProperty("srcproduct"),"retail_stg.tblproduct_stg")
    
    logger.warn("======staging process completed at " + format.format(Calendar.getInstance().getTime()))
    
    
  }
  
   def readfileandwriteintostaging(spark:SparkSession,filename:String,tablename:String)=
   {
      logger.warn("reading data from the file:" + filename)
      println("load data into hive data: " + tablename)
      val df = spark.read.format("csv")
      .option("header",true)
      .option("quote","\"")
      .option("inferSchema",true)
      .load(filename)
      
      df.show()
      
      val df1 = df.withColumn("load_dt", current_date())
      
      //write data into hive table
      df1.write.mode("overwrite").saveAsTable(tablename)
      logger.warn("written data into hive table:" + tablename)
   }
  
}