package retail.layers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import retail.driver.rundriver.logger
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions.regexp_replace
import java.util.Properties

object curationprocess {
  
  val format = new SimpleDateFormat("yyyy-MM-dd h:m:s")
  
  def curateprocess(spark:SparkSession,prop:Properties)=
  {
    spark.sql("DROP DATABASE IF EXISTS retail_curated CASCADE")
    spark.sql("create database if not exists retail_curated")
    logger.warn("====curation process started at " + format.format(Calendar.getInstance().getTime()))
    
    //Product data procecss
    val dfproduct = spark.sql("""select p.ProductKey,p.ProductName,p.ModelName,p.ProductDescription,p.ProductColor,p.ProductSize,p.ProductStyle,p.ProductCost,p.ProductPrice,
    pc.CategoryName,ps.SubcategoryName,p.load_dt from retail_stg.tblproduct_stg p inner join retail_stg.tblproductsubcategory_stg ps on p.ProductSubcategoryKey=ps.ProductSubcategoryKey
    inner join retail_stg.tblproductcategory_stg pc on ps.ProductCategoryKey = pc.ProductCategoryKey""")
    
    val dfproduct1 = dfproduct.withColumn("profit", col("ProductPrice") - col("ProductCost"))
    
    dfproduct1.write.mode("overwrite").partitionBy("load_dt").saveAsTable("retail_curated.tblproduct_dtl")
    
    logger.warn("=== data written into product table in curated database=======")
    
    
    //Customer data process
    val dfcustomer = spark.sql("""select CustomerKey,Prefix,FirstName,LastName,BirthDate,MaritalStatus,Gender,EmailAddress,AnnualIncome,TotalChildren,
                                EducationLevel,Occupation,HomeOwner,load_dt from retail_stg.tblcustomer_stg""")
    
     val dfcustomer1 = dfcustomer.withColumn("Birth_Date",to_date(col("BirthDate"),"M/d/yyyy"))
                                .withColumn("Annual_Income",regexp_replace(regexp_replace(col("AnnualIncome"),"\\$",""),",","").cast("int"))
                                .drop("BirthDate","AnnualIncome")
    
    dfcustomer1.write.mode("overwrite").partitionBy("load_dt").saveAsTable("retail_curated.tblcustomer_dtl")
    
    logger.warn("=== data written into customer table in curated database=======")
    
    //Sales data process
    val dfsales = spark.sql("""select OrderDate,StockDate,OrderNumber,ProductKey,CustomerKey,TerritoryKey,OrderLineItem,OrderQuantity,load_dt from retail_stg.tblsales_stg""")
    
    val dfsales1 = dfsales.withColumn("Order_Date",to_date(col("OrderDate"),"M/d/yyyy"))
     .withColumn("Stock_Date",to_date(col("StockDate"),"M/d/yyyy"))                      
     .drop("OrderDate","StockDate")
     
    dfsales1.write.mode("overwrite").partitionBy("load_dt").saveAsTable("retail_curated.tblsales_dtl")
    
    logger.warn("=== data written into sales table in curated database=======")
    
    
    val dfterritory = spark.sql("""select SalesTerritoryKey,Region,Country,Continent,load_dt from retail_stg.tblterritory_stg""")
    dfterritory.write.mode("overwrite").partitionBy("load_dt").saveAsTable("retail_curated.tblterritory_dtl")
    logger.warn("=== data written into territory table in curated database=======")
    
    logger.warn("====curation process completed at " + format.format(Calendar.getInstance().getTime()))
    
    
  }
}