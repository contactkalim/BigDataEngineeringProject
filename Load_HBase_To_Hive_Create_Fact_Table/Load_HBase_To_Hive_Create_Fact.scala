import scala.util.matching.Regex
import scala.util.Try
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.sql.Encoders
import java.util.HashMap
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

object Load_HBase_To_Hive_Create_Fact extends Serializable{
case class Case(status:String, category:String, sub_category:String, last_modified_timestamp:String, case_no:String, create_timestamp:String, created_employee_key:String, 
call_center_id:String, product_code:String, country_cd:String, communication_mode:String)
case class Survey(Q1:Integer, Q3:Integer, Q2:Integer, Q5:Integer, Q4:String, case_no:String, survey_timestamp:String, survey_id:String)

	def parseCaseRow(result:Result):Case ={
	val cfDataBytes = Bytes.toBytes("Case")	
	val d0 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("status")))
	val d1 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("category_key")))
	val d2 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("sub_category_key")))
	val d3 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("last_modified_timestamp")))
	val d4 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("case_no")))
	val d5 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("create_timestamp")))
	val d6 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("created_employee_key")))
	val d7 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("call_center_id")))
	val d8 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("product_code")))
	val d9 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("country_cd")))
	val d10 = Bytes.toString(result.getValue(cfDataBytes,Bytes.toBytes("communication_mode")))
	Case(d0,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10)
	}
	def parseSurveyRow(result:Result):Survey ={
	val cfDataBytes = Bytes.toBytes("Survey")	
	val d0 = Bytes.toInt(result.getValue(cfDataBytes, Bytes.toBytes("Q1")))
	val d1 = Bytes.toInt(result.getValue(cfDataBytes, Bytes.toBytes("Q3")))
	val d2 = Bytes.toInt(result.getValue(cfDataBytes, Bytes.toBytes("Q2")))
	val d3 = Bytes.toInt(result.getValue(cfDataBytes, Bytes.toBytes("Q5")))
	val d4 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("Q4")))
	val d5 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("case_no")))
	val d6 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("survey_timestamp")))
	val d7 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("survey_id")))
	Survey(d0,d1,d2,d3,d4,d5,d6,d7)
	}

  def main(args: Array[String]){
	println("Start process...")
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
	
	val spark = SparkSession.builder().appName("Load_HBase_To_Hive_Create_Pivot").enableHiveSupport().getOrCreate()
	spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
	spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
	spark.sqlContext.setConf("spark.sql.shuffle.partitions", "400")
	
	val hconf = HBaseConfiguration.create()
	hconf.set("hbase.zookeeper.quorum", "ip-20-0-31-210.ec2.internal")
	hconf.set(TableInputFormat.INPUT_TABLE,"edureka_766323_futurecart_Cases")
	
	val hbasecaseRDD = spark.sparkContext.newAPIHadoopRDD(
							hconf,
							classOf[TableInputFormat],
							classOf[ImmutableBytesWritable],
							classOf[Result]
							)	
	import spark.implicits._
	val caseResultRDD = hbasecaseRDD.map(tuple => tuple._2)
	val caseRDD = caseResultRDD.map(parseCaseRow)
	val caseStreamDF = caseRDD.toDF	
	
	hconf.set(TableInputFormat.INPUT_TABLE,"edureka_766323_futurecart_Surveys")
	println("Writing data from HBase to Hive.")
	val hbasesurveyRDD = spark.sparkContext.newAPIHadoopRDD(
							hconf,
							classOf[TableInputFormat],
							classOf[ImmutableBytesWritable],
							classOf[Result]
							)
	val surveyResultRDD = hbasesurveyRDD.map(tuple => tuple._2)
	val surveyRDD = surveyResultRDD.map(parseSurveyRow)
	val surveyStreamDF = surveyRDD.toDF
	
	caseStreamDF.createOrReplaceTempView("case_details_Hbase_data")
	surveyStreamDF.createOrReplaceTempView("case_survey_details_Hbase_data")
	
	val case_details_Hbase_data_df = spark.sql("select *, cast(create_timestamp as date) as create_date, from_unixtime(unix_timestamp()) as row_insertion_dttm from case_details_Hbase_data")
	case_details_Hbase_data_df.write.mode("overwrite").partitionBy("create_date").format("orc").saveAsTable("edureka_766323.futurecart_case_dly_incr")
	println("Created table edureka_766323.futurecart_case_dly_incr")
	
	val case_survey_details_Hbase_data_df = spark.sql("select *, cast(survey_timestamp as date) as survey_date, from_unixtime(unix_timestamp()) as row_insertion_dttm from case_survey_details_Hbase_data")
	case_survey_details_Hbase_data_df.write.mode("overwrite").partitionBy("survey_date").format("orc").saveAsTable("edureka_766323.futurecart_survey_dly_incr")
	println("Created table edureka_766323.futurecart_survey_dly_incr")
	surveyRDD.unpersist()
	caseRDD.unpersist()
	
	spark.sql("""
	create table edureka_766323.futurecart_case_dly (status string, category string, sub_category string, last_modified_timestamp string, case_no string, create_timestamp string, 
	created_employee_key string, call_center_id string, product_code string, country_cd string, communication_mode string, row_insertion_dttm timestamp) 
	PARTITIONED BY (create_date date) 
	STORED AS orc 
	"""); 

	spark.sql("""
	insert overwrite table edureka_766323.futurecart_case_dly partition(create_date) 
	select *, from_unixtime(unix_timestamp()) as row_insertion_dttm from (
	select distinct status, category, sub_category, last_modified_timestamp, case_no, create_timestamp, created_employee_key, 
	call_center_id, product_code, country_cd, communication_mode, create_date from 
	(
	select status, category, sub_category, last_modified_timestamp, case_no, create_timestamp, created_employee_key, 
	call_center_id, product_code, country_cd, communication_mode, cast(create_timestamp as date) as create_date from edureka_766323.futurecart_case_dly_incr 
	union 
	select status, category, sub_category, last_modified_timestamp, case_no, create_timestamp, created_employee_key, 
	call_center_id, product_code, country_cd, communication_mode, cast(create_timestamp as date) as create_date from edureka_766323.futurecart_case_dly_stg 
	) A ) B
	"""); 
	println("Created table edureka_766323.futurecart_case_dly")

	spark.sql("""
	create table edureka_766323.futurecart_survey_dly (Q1 int, Q3 int, Q2 int, Q5 int, Q4 string, case_no string, survey_timestamp string
	, survey_id string, row_insertion_dttm timestamp) 
	PARTITIONED BY (survey_date date) 
	STORED AS orc 
	"""); 
	spark.sql("""
	insert overwrite table edureka_766323.futurecart_survey_dly partition(survey_date) 
	select *, from_unixtime(unix_timestamp()) as row_insertion_dttm from (
	select distinct Q1, Q3, Q2, Q5, Q4, case_no, survey_timestamp, survey_id, survey_date from 
	(
	select Q1, Q3, Q2, Q5, Q4, case_no, survey_timestamp, survey_id, cast(survey_timestamp as date) as survey_date from edureka_766323.futurecart_survey_dly_incr 
	union  
	select Q1, Q3, Q2, Q5, Q4, case_no, survey_timestamp, survey_id, cast(survey_timestamp as date) as survey_date from edureka_766323.futurecart_survey_dly_stg 
	) A ) B
	"""); 
	println("Created table edureka_766323.futurecart_survey_dly")
	
	spark.sql("""
	create table edureka_766323.fact_futurecart_case_survey_dly (status string, category string, sub_category string, last_modified_timestamp timestamp, case_no string, 
	create_timestamp timestamp, created_employee_key string, call_center_id string, product_code string, country_cd string, communication_mode string,  
	Q1 int, Q3 int, Q2 int, Q5 int, Q4 string, survey_timestamp timestamp, survey_id string, row_insertion_dttm timestamp) 
	PARTITIONED BY (create_date date) 
	STORED AS orc 
	"""); 
	
	val numPartitions = 500
	spark.sql("""
	insert overwrite table edureka_766323.fact_futurecart_case_survey_dly partition(create_date) 
	select status, category, sub_category, last_modified_timestamp, C.case_no, create_timestamp, created_employee_key, 
	call_center_id, product_code, country_cd, communication_mode, Q1, Q3, Q2, Q5, Q4, survey_timestamp, survey_id, 
	from_unixtime(unix_timestamp()) as row_insertion_dttm, cast(create_timestamp as date) as create_date from 
	(select * from ( 
	select *, row_number() over(partition by case_no order by last_modified_timestamp desc) as rn from ( 
    select  status, category, sub_category, date_format(last_modified_timestamp,'yyyy-MM-dd HH:mm:ss') as last_modified_timestamp, case_no,  
	date_format(create_timestamp,'yyyy-MM-dd HH:mm:ss') as create_timestamp, created_employee_key, 
	call_center_id, product_code, country_cd, communication_mode from edureka_766323.futurecart_case_dly ) t) t1 
	where t1.rn = 1) C 
	left outer join 
	(select * from ( 
	select *, row_number() over(partition by case_no order by survey_timestamp desc) as rn from ( 
    select Q1, Q3, Q2, Q5, Q4, case_no, date_format(survey_timestamp,'yyyy-MM-dd HH:mm:ss') as survey_timestamp, survey_id from edureka_766323.futurecart_survey_dly ) t) t1 
	where t1.rn = 1) S  
	on (C.case_no = S.case_no) 
	""").repartition(numPartitions);
	println("Created table edureka_766323.fact_futurecart_case_survey_dly")
	
	println("Completed process.")

  }
}