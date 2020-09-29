import scala.util.matching.Regex
import scala.util.Try
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.streaming._
import org.apache.spark.sql.Encoders
import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import scala.util.parsing.json._
import org.apache.log4j.{Level, Logger}

object Case_Survey_Streaming{
case class Case(status:String, category_key:String, sub_category_key:String, last_modified_timestamp:String, case_no:String, create_timestamp:String, created_employee_key:String, 
call_center_id:String, product_code:String, country_cd:String, communication_mode:String)
case class Survey(Q1:Integer, Q3:Integer, Q2:Integer, Q5:Integer, Q4:String, case_no:String, survey_timestamp:String, survey_id:String)
case class Case_Survey(status:String, category:String, sub_category:String, last_modified_timestamp:String, case_no:String, create_timestamp:String, created_employee_key:String, 
call_center_id:String, product_code:String, country_cd:String, communication_mode:String, Q1:Integer, Q3:Integer, Q2:Integer, Q5:Integer, Q4:String, survey_timestamp:String, 
survey_id:String)
var i = 0
var j = 0
  def main(args: Array[String]) {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
	if (args.length < 5) {
         System.err.println("Usage: Case_Survey_Streaming <zkQuorum> <group> <topic1><topic2><numThreads>")
         System.exit(1)
      }
	val spark = SparkSession.builder().appName("Case_Survey_Streaming").enableHiveSupport().getOrCreate()
	
	val case_category_details_df = spark.read.format("jdbc").option("url", "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database").option("driver", "com.mysql.jdbc.Driver")
	.option("dbtable", "edureka_766323_futurecart_case_category_details").option("user", "edu_labuser").option("password", "edureka").load()
	val case_category_details_df_1 = case_category_details_df.selectExpr("category_key", "sub_category_key", "category_description", "sub_category_description", "priority as Priority_key" )
	case_category_details_df_1.cache()

	val case_priority_details_df = spark.read.format("jdbc").option("url", "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database").option("driver", "com.mysql.jdbc.Driver")
	.option("dbtable", "edureka_766323_futurecart_case_priority_details").option("user", "edu_labuser").option("password", "edureka").load()
	case_priority_details_df.cache()
	println("Getting MySql Case Category and Case Priority tables...")
	val case_priority_df = case_category_details_df_1.join(case_priority_details_df, case_category_details_df_1("Priority_key") === case_priority_details_df("Priority_key"), "inner")
	case_priority_df.cache()
	
    val Array(zkQuorum, group, topic1,topic2, numThreads) = args
	val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(15))
	ssc.checkpoint("checkpoint")
	val topicMapCase = topic1.split(",").map((_, numThreads.toInt)).toMap
    val Casemessage = KafkaUtils.createStream(ssc, zkQuorum, group, topicMapCase).map(_._2)
	//Casemessage.print
	val jcmessage = Casemessage.map(x =>x.replace("[", "").replace("]", ""))
	val jcmessages = jcmessage.flatMap(x =>x.split("},"))
	val jscmessages = jcmessages.map(x =>x + "}")
	val jsoncasemsgs = jscmessages.map(x =>x.replace("}}", "}"))
	//jsoncasemsgs.print
	val casemessages = jsoncasemsgs.flatMap(record => 
	{
        JSON.parseFull(record).map(rawMap => {
        	val map = rawMap.asInstanceOf[Map[String,Any]]
            Case(map.get("status").get.asInstanceOf[String], map.get("category").get.asInstanceOf[String],map.get("sub_category").get.asInstanceOf[String], 
			map.get("last_modified_timestamp").get.asInstanceOf[String],map.get("case_no").get.asInstanceOf[String], map.get("create_timestamp").get.asInstanceOf[String],
			map.get("created_employee_key").get.asInstanceOf[String], map.get("call_center_id").get.asInstanceOf[String],map.get("product_code").get.asInstanceOf[String], 
			map.get("country_cd").get.asInstanceOf[String], map.get("communication_mode").get.asInstanceOf[String])
        })
    })
	println("Processing Kafka case messages...")
	casemessages.foreachRDD{rdd =>
	if (!rdd.isEmpty)
	{
		import org.apache.spark.sql.SaveMode.Append
		import spark.implicits._		
		val cases_df = rdd.toDF
		val cases_with_priority = case_priority_df.join(cases_df, case_priority_df("category_key") === cases_df("category_key") && case_priority_df("sub_category_key") === cases_df("sub_category_key"), "rightouter")
		cases_with_priority.createOrReplaceTempView("cases_with_priority")
		println("Generating Real time KPI's")
		println("--------------------------")
		println("Total numbers of cases that are open and closed out of the number of cases received:")
		spark.sql(""" 
					select 'Total Cases' as Case_Type, count(distinct case_no) as Cases_numbers from cases_with_priority 
					union all 
					select 'Open Cases' as Case_Type, count(distinct case_no) as Cases_numbers from cases_with_priority where status = 'Open' 
					union all 
					select 'Closed Cases' as Case_Type, count(distinct case_no) as Cases_numbers from cases_with_priority where status = 'Closed' 
				 """).show(false); 
		println("Total number of cases received based on priority and severity")
		spark.sql(""" 
					select priority, severity, count(distinct case_no) as Cases_numbers from cases_with_priority 
					group by priority, severity 
				 """).show(false); 
		println("Start writing micro batch of case data to HBase...")
		val config = HBaseConfiguration.create()
		config.set(TableOutputFormat.OUTPUT_TABLE, "edureka_766323_futurecart_Cases")
		config.set("hbase.zookeeper.quorum", "ip-20-0-31-210.ec2.internal")
		config.set("hbase.zookeeper.property.clientPort", "2181")
		val jobConfig = Job.getInstance(config)
		jobConfig.setOutputFormatClass(classOf[TableOutputFormat[_]])
		
		rdd.map(record => 
		{
			i = i + 1
			val put = new Put(Bytes.toBytes(i))
			put.addColumn(Bytes.toBytes("Case"), Bytes.toBytes("status"), Bytes.toBytes(record.status))
			put.addColumn(Bytes.toBytes("Case"), Bytes.toBytes("category_key"), Bytes.toBytes(record.category_key))
			put.addColumn(Bytes.toBytes("Case"), Bytes.toBytes("sub_category_key"), Bytes.toBytes(record.sub_category_key))
			put.addColumn(Bytes.toBytes("Case"), Bytes.toBytes("last_modified_timestamp"), Bytes.toBytes(record.last_modified_timestamp))
			put.addColumn(Bytes.toBytes("Case"), Bytes.toBytes("case_no"), Bytes.toBytes(record.case_no))
			put.addColumn(Bytes.toBytes("Case"), Bytes.toBytes("create_timestamp"), Bytes.toBytes(record.create_timestamp))
			put.addColumn(Bytes.toBytes("Case"), Bytes.toBytes("created_employee_key"), Bytes.toBytes(record.created_employee_key))
			put.addColumn(Bytes.toBytes("Case"), Bytes.toBytes("call_center_id"), Bytes.toBytes(record.call_center_id))
			put.addColumn(Bytes.toBytes("Case"), Bytes.toBytes("product_code"), Bytes.toBytes(record.product_code))
			put.addColumn(Bytes.toBytes("Case"), Bytes.toBytes("country_cd"), Bytes.toBytes(record.country_cd))
			put.addColumn(Bytes.toBytes("Case"), Bytes.toBytes("communication_mode"), Bytes.toBytes(record.communication_mode))
			(new ImmutableBytesWritable(put.getRow()), put)
		}
		).saveAsNewAPIHadoopDataset(jobConfig.getConfiguration)
		println("Completed writing micro batch of case data to HBase.")
	}
	}
	
	val topicMapSurvey = topic2.split(",").map((_, numThreads.toInt)).toMap
    val Surveymessage = KafkaUtils.createStream(ssc, zkQuorum, group, topicMapSurvey).map(_._2)
	//Surveymessage.print
	val jsmessage = Surveymessage.map(x =>x.replace("[", "").replace("]", ""))
	val jsmessages = jsmessage.flatMap(x =>x.split("},"))
	val jssmessages = jsmessages.map(x =>x + "}")
	val jsonsurveymsgs = jssmessages.map(x =>x.replace("}}", "}"))
	//jsonsurveymsgs.print
	val surveymessages = jsonsurveymsgs.flatMap(record => 
	{
        JSON.parseFull(record).map(rawMap => {
        	val map = rawMap.asInstanceOf[Map[String,Any]]
            Survey(map.get("Q1").get.asInstanceOf[Integer], map.get("Q3").get.asInstanceOf[Integer],map.get("Q2").get.asInstanceOf[Integer], 
			map.get("Q5").get.asInstanceOf[Integer],map.get("Q4").get.asInstanceOf[String], map.get("case_no").get.asInstanceOf[String],
			map.get("survey_timestamp").get.asInstanceOf[String], map.get("survey_id").get.asInstanceOf[String])
        })
    })
	println("Processing Kafka Survey messages...")
	surveymessages.foreachRDD{rdd =>
	if (!rdd.isEmpty)
	{
		val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
		import org.apache.spark.sql.SaveMode.Append
		import spark.implicits._		
		val config = HBaseConfiguration.create()
		config.set(TableOutputFormat.OUTPUT_TABLE, "edureka_766323_futurecart_Surveys")
		config.set("hbase.zookeeper.quorum", "ip-20-0-31-210.ec2.internal")
		config.set("hbase.zookeeper.property.clientPort", "2181")
		val jobConfig = Job.getInstance(config)
		jobConfig.setOutputFormatClass(classOf[TableOutputFormat[_]])
		println("Start writing micro batch of survey data to HBase...")
		rdd.map(record => 
		{
			j = j + 1
			val put = new Put(Bytes.toBytes(j))
			put.addColumn(Bytes.toBytes("Survey"), Bytes.toBytes("Q1"), Bytes.toBytes(record.Q1))
			put.addColumn(Bytes.toBytes("Survey"), Bytes.toBytes("Q3"), Bytes.toBytes(record.Q3))
			put.addColumn(Bytes.toBytes("Survey"), Bytes.toBytes("Q2"), Bytes.toBytes(record.Q2))
			put.addColumn(Bytes.toBytes("Survey"), Bytes.toBytes("Q5"), Bytes.toBytes(record.Q5))
			put.addColumn(Bytes.toBytes("Survey"), Bytes.toBytes("Q4"), Bytes.toBytes(record.Q4))
			put.addColumn(Bytes.toBytes("Survey"), Bytes.toBytes("case_no"), Bytes.toBytes(record.case_no))
			put.addColumn(Bytes.toBytes("Survey"), Bytes.toBytes("survey_timestamp"), Bytes.toBytes(record.survey_timestamp))
			put.addColumn(Bytes.toBytes("Survey"), Bytes.toBytes("survey_id"), Bytes.toBytes(record.survey_id))
			(new ImmutableBytesWritable(put.getRow()), put)
		}
		).saveAsNewAPIHadoopDataset(jobConfig.getConfiguration)
		println("Completed writing micro batch of survey data to HBase.")
		
	}
	}
	
	//JOINING CASE AND SURVEY STREAMS
	val casemessagesK = casemessages.map(x => (x.case_no, x))
	val surveymessagesK = surveymessages.map(x => (x.case_no, x))
	val case_surveys = casemessagesK.join(surveymessagesK)
	case_surveys.foreachRDD{rdd =>
	if (!rdd.isEmpty)
	{
		val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
		import org.apache.spark.sql.SaveMode.Append
		import spark.implicits._
		val rddmapped = rdd.map(x => Case_Survey(x._2._1.status, x._2._1.category_key, x._2._1.sub_category_key, x._2._1.last_modified_timestamp, x._2._1.case_no, x._2._1.create_timestamp, 
												 x._2._1.created_employee_key, x._2._1.call_center_id, x._2._1.product_code, x._2._1.country_cd, x._2._1.communication_mode, 
												 x._2._2.Q1, x._2._2.Q3, x._2._2.Q2, x._2._2.Q5, x._2._2.Q4, x._2._2.survey_timestamp, x._2._2.survey_id))
		val df = rddmapped.toDF()
		val props = new Properties()
		props.put("bootstrap.servers", "ip-20-0-31-210.ec2.internal:9092")
		props.put("acks", "1")
        props.put("retries", "0")
		props.put("buffer.memory", "104857600")
        props.put("max.block.ms", "30000")
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		val producer = new KafkaProducer[String, String](props)
		val jsonmsg = df.toJSON.collect.mkString("[", "," , "]" )
		println("JOINING CASE AND SURVEY MICRO STREAM BATCH AND WRITE TO KAKFA TOPIC - edureka_766323_futurecart_case_survey_joined...")
		val record = new ProducerRecord[String, String]("edureka_766323_futurecart_case_survey_joined", null, jsonmsg)
		producer.send(record)
		println("Number of joined Records in JOSN format produced in KAFKA topic is : " + df.count)
		producer.close()
		println("COMPLETED WRITING JOINED CASE AND SURVEY MICRO STREAM BATCH TO KAKFA TOPIC  - edureka_766323_futurecart_case_survey_joined...")
		//df.show
	}
	}
	
    ssc.start()
    ssc.awaitTermination()

  }
}