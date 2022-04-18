package retail.layers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import retail.driver.rundriver.logger
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions.regexp_replace
import java.util.Properties

object aggregateprocess {
  
  val format = new SimpleDateFormat("yyyy-MM-dd h:m:s")
  def aggrprocess(spark:SparkSession,prop:Properties)=
  {
    
    logger.warn("====aggregation process started at " + format.format(Calendar.getInstance().getTime()))
   
    //product wise aggregated data
    spark.sql("DROP DATABASE IF EXISTS retail_agg CASCADE")
    spark.sql("create database if not exists retail_agg")
    
    val dfsales = spark.sql("""select s.Order_Date,sum(s.OrderQuantity) orderquantity,sum(p.ProductCost) productcost,sum(p.ProductPrice) productprice 
      from retail_curated.tblsales_dtl s inner join  retail_curated.tblproduct_dtl p on s.ProductKey = p.ProductKey group by s.Order_Date""")
      
   dfsales.write.mode("overwrite").partitionBy("Order_Date").saveAsTable("retail_agg.tbl_fact_productsales")
    
   logger.warn("=== data written into productsales table in Aggregated database=======")
   
   
   //customer wise sales data
   val dfcustomer = spark.sql("""select s.Order_Date,c.Occupation,c.MaritalStatus,sum(c.CustomerKey) customercount,sum(s.OrderQuantity) orderquantity 
      from retail_curated.tblsales_dtl s inner join  retail_curated.tblcustomer_dtl c on s.CustomerKey = c.CustomerKey 
      group by s.Order_Date,c.Occupation,c.MaritalStatus""")
    
    dfcustomer.write.mode("overwrite").partitionBy("Order_Date").saveAsTable("retail_agg.tbl_fact_customersales")
    
    logger.warn("=== data written into customersales table in Aggregated database=======")
    
    
    //Region wise sales data
    val dfterritory = spark.sql("""select s.Order_Date,t.Region,t.Country,t.Continent,sum(s.OrderQuantity) orderquantity 
      from retail_curated.tblsales_dtl s inner join retail_curated.tblterritory_dtl t on s.TerritoryKey  = t.SalesTerritoryKey
      group by s.Order_Date,t.Region,t.Country,t.Continent""")
    
    dfterritory.write.mode("overwrite").partitionBy("Order_Date").saveAsTable("retail_agg.tbl_fact_territorysales")
      
    logger.warn("=== data written into territorysales table in curated database=======")
    
    logger.warn("====aggregation process completed at " + format.format(Calendar.getInstance().getTime()))
   
   
   
   
    
  }
  
}