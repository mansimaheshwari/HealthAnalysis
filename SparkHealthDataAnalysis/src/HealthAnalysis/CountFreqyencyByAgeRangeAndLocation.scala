

package HealthAnalysis

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.ForeachWriter
import java.io.FileWriter
import org.apache.spark.sql.Row
import org.apache.commons.io.FileUtils
import java.io.File
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SaveMode

object CountFreqyencyByAgeRangeAndLocation {
  
  def main(args : Array[String]){
    
    
//  configurations for SparkSession.builder()
      def sparkConf : SparkConf={
        val sConf=new SparkConf
        sConf.set("spark.app.name", "CountFreqyencyByAgeRangeAndLocation")
        sConf.set("spark.master","local[*]")
        sConf // == return statement
      }
      
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val spark=SparkSession.builder()                      
                            .config(sparkConf)
                            .config("spark.sql.shuffle.partitions","1")    // to make the no of partitions to 3 instead of 200 (spark default)
                            .getOrCreate()
    

                       
       import spark.implicits._  
       val dfDemographic=spark.read
                  .format("csv")
                  .option("path","inputFiles/demographic.csv")
                  .option("header",true)
                  .schema("seqn Integer, familyMember Integer, location Integer, income Integer, insuranceAmount Double, age Integer")  //  .option("inferSchema",true)
                  .load
                  .drop("familyMember", "income", "insuranceAmount")
                  

       val dfAgeRange=spark.read
                  .format("csv")
                  .option("path","inputFiles/AgeRanges.csv")
                  .option("header",true)
                  .option("inferSchema",true)
                  .load                      
                  
                  
       import org.apache.spark.sql.functions._  // import to use agg() function
       
       val joinCond= dfDemographic("age")<=dfAgeRange("max") && dfDemographic("age")>=dfAgeRange("min")
       val joinType="inner"                                                                                          
       val join=dfDemographic.join((dfAgeRange),joinCond,joinType)
                              .drop(dfAgeRange("min"))
                              .drop(dfAgeRange("max"))
                              .drop(dfDemographic("seqn"))
                              .drop(dfDemographic("age"))
//                              .select("*")
//                              .show()

                              
//      creating pivot table
      join.createOrReplaceTempView("table")
      val out=spark.sql("""SELECT  ageRange , 
                                 SUM(CASE WHEN (location=1) THEN 1 ELSE 0 END) AS location1, 
                                 SUM(CASE WHEN (location=2) THEN 1 ELSE 0 END) AS location2, 
                                 SUM(CASE WHEN (location=3) THEN 1 ELSE 0 END) AS location3, 
                                 SUM(CASE WHEN (location=4) THEN 1 ELSE 0 END) AS location4,
                                 SUM(CASE WHEN (location=5) THEN 1 ELSE 0 END) AS location5
                         from table  group by ageRange order by ageRange""")
//                      .show(10)
    

      out.write
          .format("csv")
          .mode(SaveMode.Overwrite)
          .option("header", true)
          .option("path","outputFiles/CountFreqyencyByAgeRangeAndLocation")
          .save
                                  
      scala.io.StdIn.readLine()
      spark.close()
  }
}