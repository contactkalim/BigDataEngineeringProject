import scala.util.Try
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.apache.spark.sql.Encoders
import org.apache.log4j.{Level, Logger}

object Load_Files_To_Mysql {
  def main(args: Array[String]) {
	val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
	
    val spark = SparkSession.builder().appName("SparkSessionZipsExample").enableHiveSupport().getOrCreate()
	println("Reading text files...")
	val calendar_details = spark.read.format("csv").option("sep","\t").option("header","true").load("hdfs:///user/edureka_766323/batchdata/futurecart_calendar_details.txt")
	val call_center_details = spark.read.format("csv").option("sep","\t").option("header","true").load("hdfs:///user/edureka_766323/batchdata/futurecart_call_center_details.txt")
	val case_category_details = spark.read.format("csv").option("sep","\t").option("header","true").load("hdfs:///user/edureka_766323/batchdata/futurecart_case_category_details.txt")
	val case_country_details = spark.read.format("csv").option("sep","\t").option("header","true").load("hdfs:///user/edureka_766323/batchdata/futurecart_case_country_details.txt")
	val case_priority_details = spark.read.format("csv").option("sep","\t").option("header","true").load("hdfs:///user/edureka_766323/batchdata/futurecart_case_priority_details.txt")
	val employee_details = spark.read.format("csv").option("sep","\t").option("header","true").load("hdfs:///user/edureka_766323/batchdata/futurecart_employee_details.txt")
	val product_details = spark.read.format("csv").option("sep","\t").option("header","true").load("hdfs:///user/edureka_766323/batchdata/futurecart_product_details.txt")
	val survey_question_details = spark.read.format("csv").option("sep","\t").option("header","true").load("/user/edureka_766323/batchdata/futurecart_survey_question_details.txt")
	val case_details = spark.read.format("csv").option("sep","\t").option("header","true").load("hdfs:///user/edureka_766323/batchdata/futurecart_case_details.txt")
	val case_survey_details = spark.read.format("csv").option("sep","\t").option("header","true").load("hdfs:///user/edureka_766323/batchdata/futurecart_case_survey_details.txt")

	val prop = new java.util.Properties
	prop.setProperty("driver", "com.mysql.jdbc.Driver")
	prop.setProperty("user", "edu_labuser")
	prop.setProperty("password", "edureka") 

	val calendar_details_df = calendar_details.selectExpr("cast(calendar_date as date) calendar_date", "cast(date_desc as string) date_desc", "cast(week_day_nbr as int) week_day_nbr", 
	"cast(week_number as int) week_number", "cast(week_name as string) week_name", "cast(year_week_number as int) year_week_number", "cast(month_number as int) month_number" , 
	"cast(month_name as string) month_name" , "cast(quarter_number as int) quarter_number", "cast(quarter_name as string) quarter_name", "cast(half_year_number as int) half_year_number",
	 "cast(half_year_name as string) half_year_name", "cast(geo_region_cd as string) geo_region_cd" )
	 
	 val call_center_details_df = call_center_details.selectExpr("cast(call_center_id as string) call_center_id", "cast(call_center_vendor as string) call_center_vendor", 
	 "cast(location as string) location", "cast(country as string) country" )
	 
	 val case_category_details_df = case_category_details.selectExpr("cast(category_key as string) category_key", "cast(sub_category_key as string) sub_category_key", 
	 "cast(category_description as string) category_description", "cast(sub_category_description as string) sub_category_description", "cast(priority as string) priority" )
	 
	 val case_country_details_df = case_country_details.selectExpr("cast(id as int) id", "cast(Name as string) Name", "cast(Alpha_2 as string) Alpha_2", "cast(Alpha_3 as string) Alpha_3" )
	 
	 val case_priority_details_df = case_priority_details.selectExpr("cast(Priority_key as string) Priority_key", "cast(priority as string) priority", 
	 "cast(severity as string) severity", "cast(SLA as string) SLA" )
	 
	 val employee_details_df = employee_details.selectExpr("cast(emp_key as int) emp_key", "cast(first_name as string) first_name", "cast(last_name as string) last_name", 
	"cast(email as string) email", "cast(gender as string) gender", "cast(ldap as string) ldap", "cast(hire_date as date) hire_date" , "cast(manager as string) manager" )
	 
	 val product_details_df = product_details.selectExpr("cast(product_id as string) product_id", "cast(department as string) department", 
	 "cast(brand as string) brand", "cast(commodity_desc as string) commodity_desc", "cast(sub_commodity_desc as string) sub_commodity_desc" )
	 
	 val survey_question_details_df = survey_question_details.selectExpr("cast(question_id as string) question_id", "cast(question_desc as string) question_desc", 
	 "cast(response_type as string) response_type", "cast(range as string) range", "cast(negative_response_range as string) negative_response_range", 
	 "cast(neutral_response_range as string) neutral_response_range", "cast(positive_response_range as string) positive_response_range" )
	 
	  val case_details_df = case_details.selectExpr("cast(case_no as string) case_no", "cast(create_timestamp as string) create_timestamp", 
	 "cast(last_modified_timestamp as string) last_modified_timestamp", "cast(created_employee_key as string) created_employee_key", "cast(call_center_id as string) call_center_id", 
	 "cast(status as string) status", "cast(category as string) category", "cast(sub_category as string) sub_category", "cast(communication_mode as string) communication_mode", 
	 "cast(country_cd as string) country_cd", "cast(product_code as string) product_code", "cast(create_timestamp as date) as create_date" )
	 
	  val case_survey_details_df = case_survey_details.selectExpr("cast(survey_id as string) survey_id", "cast(Case_no as string) Case_no", 
	 "cast(survey_timestamp as string) survey_timestamp", "cast(Q1 as string) Q1", "cast(Q2 as string) Q2", 
	 "cast(Q3 as string) Q3", "cast(Q4 as string) Q4", "cast(Q5 as string) Q5", "cast(survey_timestamp as date) as survey_date" )
	 
	val url = "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" 
	
	println("Creating MySql table - edureka_766323_futurecart_calendar_details")
	var table = "edureka_766323_futurecart_calendar_details" 
	calendar_details_df.write.jdbc(url, table, prop)

	println("Creating MySql table - edureka_766323_futurecart_call_center_details")
	table = "edureka_766323_futurecart_call_center_details" 
	call_center_details_df.write.jdbc(url, table, prop)

	println("Creating MySql table - edureka_766323_futurecart_case_category_details")
	table = "edureka_766323_futurecart_case_category_details" 
	case_category_details_df.write.jdbc(url, table, prop)

	println("Creating MySql table - edureka_766323_futurecart_case_country_details")
	table = "edureka_766323_futurecart_case_country_details" 
	case_country_details_df.write.jdbc(url, table, prop)

	println("Creating MySql table - edureka_766323_futurecart_case_priority_details")
	table = "edureka_766323_futurecart_case_priority_details" 
	case_priority_details_df.write.jdbc(url, table, prop)
	
	println("Creating MySql table - edureka_766323_futurecart_employee_details")
	table = "edureka_766323_futurecart_employee_details" 
	employee_details_df.write.jdbc(url, table, prop)

	println("Creating MySql table - edureka_766323_futurecart_product_details")
	table = "edureka_766323_futurecart_product_details" 
	product_details_df.write.jdbc(url, table, prop)

	println("Creating MySql table - edureka_766323_futurecart_survey_question_details")
	table = "edureka_766323_futurecart_survey_question_details" 
	survey_question_details_df.write.jdbc(url, table, prop)

	println("Creating MySql table - edureka_766323_futurecart_case_details")
	table = "edureka_766323_futurecart_case_details" 
	case_details_df.write.jdbc(url, table, prop)

	println("Creating MySql table - edureka_766323_futurecart_case_survey_details")
	table = "edureka_766323_futurecart_case_survey_details" 
	case_survey_details_df.write.jdbc(url, table, prop)
  }
}






