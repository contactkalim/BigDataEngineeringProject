import scala.util.Try
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.apache.spark.sql.Encoders
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel;

object Load_MySql_Tables_To_Hive {
  def main(args: Array[String]) {
	val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
	
    val spark = SparkSession.builder().appName("Load_MySql_Tables_To_Hive").enableHiveSupport().getOrCreate()
	println("Reading MySql tables and creating Hive tables...")
	
	spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
	spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
	
	import spark.implicits._;
	val calendar_details_df = spark.read.format("jdbc").option("url", "jdbc:mysql://db_url").option("driver", "com.mysql.jdbc.Driver")
	.option("dbtable", "edureka_766323_futurecart_calendar_details").option("user", "pwd").option("password", "password").load()
	val calendar_details_df_p = calendar_details_df.persist(StorageLevel.MEMORY_AND_DISK);
	calendar_details_df_p.createOrReplaceTempView("calendar_details")
	val calendar_details_df_1 = spark.sql("select *, from_unixtime(unix_timestamp()) as row_insertion_dttm, cast(from_unixtime(unix_timestamp()) as date) as create_date from calendar_details")
	calendar_details_df_1.write.mode("overwrite").partitionBy("create_date").format("orc").saveAsTable("username.dim_futurecart_calendar")

	val call_center_details_df = spark.read.format("jdbc").option("url", "jdbc:mysql://db_url").option("driver", "com.mysql.jdbc.Driver")
	.option("dbtable", "edureka_766323_futurecart_call_center_details").option("user", "pwd").option("password", "password").load()
	val call_center_details_df_p = call_center_details_df.persist(StorageLevel.MEMORY_AND_DISK);
	call_center_details_df_p.createOrReplaceTempView("call_center_details")
	val call_center_details_df_1 = spark.sql("select *, from_unixtime(unix_timestamp()) as row_insertion_dttm, cast(from_unixtime(unix_timestamp()) as date) as create_date from call_center_details")
	call_center_details_df_1.write.mode("overwrite").partitionBy("create_date").format("orc").saveAsTable("username.dim_futurecart_call_center")

	val case_category_details_df = spark.read.format("jdbc").option("url", "jdbc:mysql://db_url").option("driver", "com.mysql.jdbc.Driver")
	.option("dbtable", "edureka_766323_futurecart_case_category_details").option("user", "pwd").option("password", "password").load()
	val case_category_details_df_p = case_category_details_df.persist(StorageLevel.MEMORY_AND_DISK);
	case_category_details_df_p.createOrReplaceTempView("case_category_details")
	val case_category_details_df_1 = spark.sql("select *, from_unixtime(unix_timestamp()) as row_insertion_dttm, cast(from_unixtime(unix_timestamp()) as date) as create_date from case_category_details")
	case_category_details_df_1.write.mode("overwrite").partitionBy("create_date").format("orc").saveAsTable("username.dim_futurecart_case_category")

	val case_country_details_df = spark.read.format("jdbc").option("url", "jdbc:mysql://db_url").option("driver", "com.mysql.jdbc.Driver")
	.option("dbtable", "edureka_766323_futurecart_case_country_details").option("user", "pwd").option("password", "password").load()
	val case_country_details_df_p = case_country_details_df.persist(StorageLevel.MEMORY_AND_DISK);
	case_country_details_df_p.createOrReplaceTempView("case_country_details")
	val case_country_details_df_1 = spark.sql("select *, from_unixtime(unix_timestamp()) as row_insertion_dttm, cast(from_unixtime(unix_timestamp()) as date) as create_date from case_country_details")
	case_country_details_df_1.write.mode("overwrite").partitionBy("create_date").format("orc").saveAsTable("username.dim_futurecart_country")

	val case_priority_details_df = spark.read.format("jdbc").option("url", "jdbc:mysql://db_url").option("driver", "com.mysql.jdbc.Driver")
	.option("dbtable", "edureka_766323_futurecart_case_priority_details").option("user", "pwd").option("password", "password").load()
	val case_priority_details_df_p = case_priority_details_df.persist(StorageLevel.MEMORY_AND_DISK);
	case_priority_details_df_p.createOrReplaceTempView("case_priority_details")
	val case_priority_details_df_1 = spark.sql("select *, from_unixtime(unix_timestamp()) as row_insertion_dttm, cast(from_unixtime(unix_timestamp()) as date) as create_date from case_priority_details")
	case_priority_details_df_1.write.mode("overwrite").partitionBy("create_date").format("orc").saveAsTable("username.dim_futurecart_case_priority")

	val employee_details_df = spark.read.format("jdbc").option("url", "jdbc:mysql://db_url").option("driver", "com.mysql.jdbc.Driver")
	.option("dbtable", "edureka_766323_futurecart_employee_details").option("user", "pwd").option("password", "password").load()
	val employee_details_df_p = employee_details_df.persist(StorageLevel.MEMORY_AND_DISK);
	employee_details_df_p.createOrReplaceTempView("employee_details")
	val employee_details_df_1 = spark.sql("select *, from_unixtime(unix_timestamp()) as row_insertion_dttm from employee_details")
	employee_details_df_1.write.mode("overwrite").partitionBy("hire_date").format("orc").saveAsTable("username.dim_futurecart_employee")

	val product_details_df = spark.read.format("jdbc").option("url", "jdbc:mysql://db_url").option("driver", "com.mysql.jdbc.Driver")
	.option("dbtable", "edureka_766323_futurecart_product_details").option("user", "pwd").option("password", "password").load()
	val product_details_df_p = product_details_df.persist(StorageLevel.MEMORY_AND_DISK);
	product_details_df_p.createOrReplaceTempView("product_details")
	val product_details_df_1 = spark.sql("select *, from_unixtime(unix_timestamp()) as row_insertion_dttm, cast(from_unixtime(unix_timestamp()) as date) as create_date from product_details")
	product_details_df_1.write.mode("overwrite").partitionBy("create_date").format("orc").saveAsTable("username.dim_futurecart_product")

	val survey_question_details_df = spark.read.format("jdbc").option("url", "jdbc:mysql://db_url").option("driver", "com.mysql.jdbc.Driver")
	.option("dbtable", "edureka_766323_futurecart_survey_question_details").option("user", "pwd").option("password", "password").load()
	val survey_question_details_df_p = survey_question_details_df.persist(StorageLevel.MEMORY_AND_DISK);
	survey_question_details_df_p.createOrReplaceTempView("survey_question_details")
	val survey_question_details_df_1 = spark.sql("select *, from_unixtime(unix_timestamp()) as row_insertion_dttm, cast(from_unixtime(unix_timestamp()) as date) as create_date from survey_question_details")
	survey_question_details_df_1.write.mode("overwrite").partitionBy("create_date").format("orc").saveAsTable("username.dim_futurecart_questions")
	
	println("Completed creating Hive tables.")
  }
}
