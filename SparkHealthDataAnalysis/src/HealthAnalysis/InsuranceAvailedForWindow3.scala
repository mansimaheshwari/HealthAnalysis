package HealthAnalysis

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window


object InsuranceAvailedForWindow3 {
  
   def sparkConf : SparkConf={
        val sConf=new SparkConf
        sConf.set("spark.app.name", "Insurance Availed For Window size = 3")
        sConf.set("spark.master","local[*]")
        sConf // == return statement
      }
      
  def main(args : Array[String]){
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger(getClass.getName).error("Insurance Availed For Window size = 3")
      
        val spark=SparkSession.builder()                      
                            .config(sparkConf)
                            .getOrCreate()
    
        
       

       val window=Window.partitionBy("location")
                         .orderBy("location")
//                         .rowsBetween(Window.unboundedPreceding,Window.currentRow)
                         .rowsBetween(-3,Window.currentRow)

       val dfDemographic=spark.read
                  .format("csv")
                  .option("path","D:/HealthDataSpark/input/demographic.csv")
                  .option("header",true)
                  .schema("seqn Integer, familyMember Integer, location Integer, income Integer, insuranceAmount Double")  //  .option("inferSchema",true)
                  .load
                  .na.fill(0,Array("insuranceAmount"))
                  .drop("familyMember")
                  .drop("income")
                  
                  
       import org.apache.spark.sql.functions._  // import to use avg() function
       val out=dfDemographic.withColumn("runningAverage3Col", avg("insuranceAmount").over(window))
//                  .show(10)
                                   
                
                    
      out.write
          .format("csv")
          .mode(SaveMode.Overwrite)
          .option("header", true)
          .option("path","D:/HealthDataSpark/output/InsuranceAvailedForWindow3")
          .save
      scala.io.StdIn.readLine()
      spark.close()
  }
  
}