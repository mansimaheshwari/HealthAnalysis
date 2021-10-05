package HealthAnalysis

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode


object Disease_AverageNutrientIntake {
  
   def sparkConf : SparkConf={
        val sConf=new SparkConf
        sConf.set("spark.app.name", "Disease and Average Nutrient Intake")
        sConf.set("spark.master","local[*]")
        sConf // == return statement
      }
      
  def main(args : Array[String]){
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger(getClass.getName).error("Disease and Average Nutrient Intake")
      
        val spark=SparkSession.builder()                      
                            .config(sparkConf)
                            .getOrCreate()
    
        
       
       import spark.implicits._  
       import org.apache.spark.sql.functions._  // import to use expr() in withColumn() function
       val dfMedications=spark.read
                  .format("csv")
                  .option("path","D:/HealthDataSpark/input/medications.csv")
                  .option("header",true)
                  .schema("seqn String,noOfMedicines Integer,drugName String")  //  .option("inferSchema",true)
                  .load
                  .filter(x=> !x.isNullAt(2))
                  .flatMap(x=>x.getString(2).split(";").map((x.getString(0),_)))
                  .toDF("seqn","drugName")

//       val rdd1=spark.sparkContext.textFile("D:/HealthDataSpark/input/medications.csv")
//                                   .map(x=>(x.split(",")(0),x.split(",")(2)))
//                                   .filter(x=> x._2.nonEmpty)
//                                   .flatMapValues(x=>x.split(";"))
//                                   .take(10)
//                                   .foreach(println)
                                   
                         


                       

       val dfDemographic=spark.read
                  .format("csv")
                  .option("path","D:/HealthDataSpark/input/diet.csv")
                  .option("header",true)
                  .schema("seqn String,carbohyderates Double,sugar Double,calcium Double, waterIntake Double")  //  .option("inferSchema",true)
                  .load
//                  .printSchema

                  
//       spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")
       
       val joinCond=dfDemographic("seqn")===dfMedications("seqn")
       val joinType="inner"                                                                                          
       val join= dfDemographic.join((dfMedications),joinCond,joinType)
                              .drop(dfDemographic("seqn"))
                              .drop(dfMedications("seqn"))
                              

      join.createOrReplaceTempView("Drugs")
      val out = spark.sql("""select drugName DrugName, avg(carbohyderates) as AverageCarbohyderates, 
                                  avg(sugar) as AverageSugar,
                                  avg(calcium) as AverageCalcium, 
                                  avg(waterIntake) as AverageWaterIntake 
                                  from Drugs 
                                  group by drugName 
                                  order by drugName asc""")
//                      .show(10)
                  
                  
                
                    
      out.write
          .format("csv")
          .mode(SaveMode.Overwrite)
          .option("path","D:/HealthDataSpark/output/Disease_AverageNutrientIntake")
          .save
      scala.io.StdIn.readLine()
      spark.close()
  }
  
}