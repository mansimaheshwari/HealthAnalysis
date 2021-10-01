package HealthAnalysis

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode


object DemographicsPartitionByLocation {
  
  def main(args : Array[String]){
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger(getClass.getName).error("Spark Object Word Count")
      
    //  val spark=SparkSession.builder()
    //                        .appName("Spark Object Word Count")
    //                        .master("local[*]")
    //                        .getOrCreate()
         
      def sparkConf : SparkConf={
        val sConf=new SparkConf
        sConf.set("spark.app.name", "Partitioning File")
        sConf.set("spark.master","local[*]")
        sConf // == return statement
      }
      
        val spark=SparkSession.builder()                      
                            .config(sparkConf)
                            .getOrCreate()
    
      val s=spark.read
                  .format("csv")
                  .option("path","D:/HealthDataSpark/input/demographic.csv")
                  .option("header",true)
                  .option("inferSchema",true)
                  .load
//                  .printSchema
                  
      s.createOrReplaceTempView("demographics")
      val out = spark.sql("select * from demographics")
//                    .show(1) 
                
                    
      out.write
          .format("json")
          .mode(SaveMode.Overwrite)
          .partitionBy("location")
          .option("path","D:/HealthDataSpark/output/DemographicsPartitionByLocation")
          .save
      scala.io.StdIn.readLine()
      spark.close()
  }
  
}