package HealthAnalysis

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window


object AverageInsuranceAvailedLocationWise {
  
   def sparkConf : SparkConf={
        val sConf=new SparkConf
        sConf.set("spark.app.name", "Insurance Availed For particular location")
        sConf.set("spark.master","local[*]")
        sConf // == return statement
      }
      
  def main(args : Array[String]){
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger(getClass.getName).error("Insurance Availed For particular location")
      
        val spark=SparkSession.builder()                      
                            .config(sparkConf)
                            .config("spark.sql.shuffle.partitions","10")    // to make the no of partitions to 3 instead of 200 (spark default)
                            .getOrCreate()
    
        
       val dfDemographic=spark.read
                  .format("csv")
                  .option("path","D:/HealthDataSpark/input/demographic.csv")
                  .option("header",true)
                  .schema("seqn Integer, familyMember Integer, location Integer, income Integer, insuranceAmount Double")  //  .option("inferSchema",true)
                  .load
                  .na.fill(0,Array("insuranceAmount"))
                  .drop("familyMember")
                  .drop("income")
                  .drop("seqn")
                  
                  
       import org.apache.spark.sql.functions._  // import to use avg() function
       val out=dfDemographic.groupBy("location")
                             .agg(expr("avg(insuranceAmount)"))
                             .orderBy("location")
                             .show()
                                   
                
//      out.write
//          .format("csv")
//          .mode(SaveMode.Overwrite)
//          .option("header", true)
//          .option("path","D:/HealthDataSpark/output/InsuranceAvailedForWindow3")
//          .save
      scala.io.StdIn.readLine()
      spark.close()
  }
  
}