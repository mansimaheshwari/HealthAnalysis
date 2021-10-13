package HealthAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

object DiseaseFrequencyInVeganAndNonVegan {
  
  
  def main(args : Array[String]){
    
    
//  configurations for SparkSession.builder()
      def sparkConf : SparkConf={
        val sConf=new SparkConf
        sConf.set("spark.app.name", "DiseaseFrequencyInVeganAndNonVegan")
        sConf.set("spark.master","local[*]")
        sConf // == return statement
      }
      
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val spark=SparkSession.builder()                      
                            .config(sparkConf)
                            .config("spark.sql.shuffle.partitions","10")    // to make the no of partitions to 3 instead of 200 (spark default)
                            .getOrCreate()
    

                  
       import spark.implicits._   // to used operations on df like filter etc.
       
       val dfMedications=spark.read
                  .format("csv")
                  .option("path","inputFiles/medications.csv")
                  .option("header",true)
                  .schema("seqn String,noOfMedicines Integer,drugName String")  //  .option("inferSchema",true)
                  .load
                  .drop("noOfMedicines")
                  .filter(x=> !x.isNullAt(1))
                  .flatMap(x=>x.getString(1).split(";").map((x.getString(0),_,1)))
                  .toDF("seqn","drugName","frequency")
                  
                  
      import org.apache.spark.sql.functions._  // import to use agg() function
      val dfQuestionnaire=spark.read
                  .format("csv")
                  .option("path","inputFiles/questionnaire.csv")
                  .option("header",true)
                  .schema("seqn String, rooms Integer, milkDiet Integer")  //  .option("inferSchema",true)
                  .load
                  .drop("rooms")     
                  .filter(x=> !x.isNullAt(1))
//                  changing column value to either 0 or 1 to indicate vegan or not
                  .withColumn("milkDiet", expr("CASE WHEN (milkDiet>0) THEN 1 ELSE 0 END"))           
                  
       
       val joinCond= dfQuestionnaire("seqn")===dfMedications("seqn")
       val joinType="inner"                                                                                          
       val join=dfQuestionnaire.join((dfMedications),joinCond,joinType)
                              .drop(dfMedications("seqn"))
                              .groupBy("drugName")
                              .pivot("milkDiet")
                              .agg(sum("frequency"))
//                              renaming the columns
                              .withColumnRenamed("1", "vegan")
                              .withColumnRenamed("0", "nonVegan")
//                              adding coalesce for null values
                              .withColumn("vegan", expr("coalesce(vegan,0)"))
                              .withColumn("nonVegan", expr("coalesce(nonVegan,0)"))
//                              .select("drugName", "nonvegan", "vegan")
                              .sort("drugName")
//                              .show(10)

      join.write
          .format("csv")
          .mode(SaveMode.Overwrite)
          .option("header", true)
          .option("path","outputFiles/DiseaseFrequencyInVeganAndNonVegan")
          .save
                                  
      scala.io.StdIn.readLine()
      spark.close()
  }
}