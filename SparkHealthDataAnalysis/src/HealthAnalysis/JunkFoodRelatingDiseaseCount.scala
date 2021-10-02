package HealthAnalysis

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode


object JunkFoodRelatingDiseaseCount {
  
   def sparkConf : SparkConf={
        val sConf=new SparkConf
        sConf.set("spark.app.name", "Junk Food Relating Disease Count")
        sConf.set("spark.master","local[*]")
        sConf // == return statement
      }
      
  def main(args : Array[String]){
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger(getClass.getName).error("Junk Food Relating Disease Count")
      
        val spark=SparkSession.builder()                      
                            .config(sparkConf)
                            .getOrCreate()
    
        

                       
       import spark.implicits._  
       val dfMedications=spark.read
                  .format("csv")
                  .option("path","D:/HealthDataSpark/input/medications.csv")
                  .option("header",true)
                  .schema("seqn String,noOfMedicines Integer,drugName String")  //  .option("inferSchema",true)
                  .load
                  .filter(x=> !x.isNullAt(2))
//                  .map(x=>(x.getString(0),x.getString(2)))
//                  .flatMap(x=>x._2.split(";"))
                  .flatMap(x=>x.getString(2).split(";").map((x.getString(0),_,1)))
                  .toDF("seqn","drugName","frequency")
//            .select("*")  
//             .show(5)
                  

       val dfQuestionnaire=spark.read
                  .format("csv")
                  .option("path","D:/HealthDataSpark/input/questionnaire.csv")
                  .option("header",true)
                  .schema("seqn String, rooms Integer, milkDiet Integer, junkFoodFrequency Integer")  //  .option("inferSchema",true)
                  .load
//            .selectExpr("junkFoodFrequency", "coalesce(junkFoodFrequency,0)") 
                  .na.fill(0,Array("junkFoodFrequency"))
//            .withColumn("junkFoodFrequency", expr("coalesce(junkFoodFrequency,0)"))
                  .drop("rooms")
                  .drop("milkDiet")

//             .show(5)

                  
       import org.apache.spark.sql.functions._  // import to use agg() function
       val joinCond=dfQuestionnaire("seqn")===dfMedications("seqn")
       val joinType="inner"                                                                                          
       val out=dfQuestionnaire.join((dfMedications),joinCond,joinType)
                              .drop(dfQuestionnaire("seqn"))
                              .drop(dfMedications("seqn"))
                              .groupBy("drugName")
                              .agg(avg("junkFoodFrequency"))
                              .orderBy("drugName")
//                      .show(10)

      out.write
          .format("csv")
          .mode(SaveMode.Overwrite)
          .option("path","D:/HealthDataSpark/output/JunkFoodRelatingDiseaseCount")
          .save
                                  
      scala.io.StdIn.readLine()
      spark.close()
  }
  
}