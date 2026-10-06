import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day13App {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day13-SparkSQL")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val customers = Seq(
      ("C1", "Ali", 25, "Hyderabad", 50000),
      ("C2", "Rahul", 30, "Delhi", 70000),
      ("C3", "Sara", 28, "Mumbai", 60000),
      ("C4", "John", 35, "Hyderabad", 90000)
    ).toDF("id", "name", "age", "city", "salary")

    println("Schema:")
    customers.printSchema()

    println("Hyderabad Customers:")
    customers.filter($"city" === "Hyderabad").show()

    val updated = customers.withColumn(
      "salary_band",
      when($"salary" >= 80000, "HIGH")
        .when($"salary" >= 60000, "MEDIUM")
        .otherwise("LOW")
    )

    updated.show()

    updated.createOrReplaceTempView("customers")

    val result = spark.sql(
      """
        |SELECT city, COUNT(*) AS customer_count,
        |AVG(salary) AS average_salary
        |FROM customers
        |GROUP BY city
        |ORDER BY average_salary DESC
        |""".stripMargin)

    result.show()

    spark.stop()
  }
}
