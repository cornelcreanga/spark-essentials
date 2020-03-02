package part7bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import part2dataframes.Joins.salariesDF
import part4sql.SparkSql.carsDF

object TaxiExercise1 extends App {

  val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Taxi Big Data Application")
      .getOrCreate()


  //val bigTaxiDF = spark.read.load("path/to/your/dataset/NYC_taxi_2009-2016.parquet")

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiDF.printSchema()

  val taxiZonesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/taxi_zones.csv")
  taxiZonesDF.printSchema()

  val groupedByPickup = taxiDF.
      groupBy(col("PULocationID")).
      agg(count("*").alias("tripNumbers")).
      orderBy(col("tripNumbers").desc).limit(15)

  val maxPickups = taxiZonesDF.join(groupedByPickup, col("PULocationID")===col("LocationID")).
      orderBy(col("tripNumbers").desc).
      drop("PULocationID")
  maxPickups.show()

  val groupedByPickupDate = taxiDF.
      groupBy(hour(col("tpep_pickup_datetime"))).
      agg(count("*").alias("tripNumbers")).
      orderBy(col("tripNumbers").desc).limit(15)

  groupedByPickupDate.show()
  taxiDF.show()
  //  taxiDF.createOrReplaceTempView("taxi")
//
//  spark.sql(
//    """
//      |select max(t.PULocationID),count(*)
//      |from taxi t
//      |group by t.PULocationID
//    """.stripMargin
//  ).show()


  /**
  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, dept_emp de, salaries s
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      |group by de.dept_no
    """.stripMargin
  )
   */


  /**
   * Questions:
   *
   * 1. Which zones have the most pickups/dropoffs overall?
   * 2. What are the peak hours for taxi?
   * 3. How are the trips distributed by length? Why are people taking the cab?
   * 4. What are the peak hours for long/short trips?
   * 5. What are the top 3 pickup/dropoff zones for long/short trips?
   * 6. How are people paying for the ride, on long/short trips?
   * 7. How is the payment type evolving with time?
   * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
   *
   */


  /**
  root
 |-- VendorID: integer (nullable = true)
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- tpep_dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: integer (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- payment_type: integer (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)

 root
 |-- LocationID: integer (nullable = true)
 |-- Borough: string (nullable = true)
 |-- Zone: string (nullable = true)
 |-- service_zone: string (nullable = true)
   */
}
