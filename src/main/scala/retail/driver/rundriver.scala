package retail.driver
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.log4j.PropertyConfigurator
import java.util.Properties
import java.io.FileInputStream

import retail.layers._


object rundriver {
  
  val logger = Logger.getLogger(this.getClass.getName)
  
  def main(args:Array[String])=
  {
    try
    {
      
      //Load the log4j configuration file
      PropertyConfigurator.configure("log4j.properties")
      
      //Load properties files into properties object
      val confdata = new FileInputStream("app.properties")
      val prop = new Properties()
      prop.load(confdata)
      
      //To capture the process start date
      val format = new SimpleDateFormat("yyyy-MM-dd h:m:s")
      val starttime = Calendar.getInstance().getTimeInMillis()
      logger.warn("======process started at " + format.format(starttime))
      
      val spark = SparkSession.builder()
      .master("local")
      .config("hive.metastore.uris","thrift://localhost:9083")
      .appName("Retail-coreengine")
      .config("spark.sql.debug.maxToStringFields", 1000)
      .enableHiveSupport()
      .getOrCreate()
      
      spark.sparkContext.setLogLevel("WARN")
      
      
      //===================staging load==========================
      stagingprocess.stageprocess(spark,prop)
      
      //==================curation process======================
      //curationprocess.curateprocess(spark,prop)
      
       //==================aggregation load =====================
      //aggregateprocess.aggrprocess(spark,prop)
      
      
    
    }
    catch
    {
          // Catch block contain cases. 
          case ex: Exception => 
          {
              println("Error Occured:" + ex.printStackTrace())
              logger.warn("Error Occured:" + ex.getMessage)
          }
        }
  }
  
}