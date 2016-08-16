package org.vincentg.spark.apps;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.vincentg.hadoop.io.MQIFileInputFormat;
import org.vincentg.mqi.constants.MQIConstants;
import org.vincentg.mqi.enums.EnumInputType;
import org.vincentg.mqi.enums.EnumOuputType;
import org.vincentg.mqi.enums.EnumRecType;
import org.vincentg.mqi.record.RecordProcessor;
import org.vincentg.mqi.record.RecordProcessorFactory;
import org.vincentg.mqi.util.MQIShutdownHook;
import org.vincentg.mqi.util.MQIUtil;
import org.vincentg.spark.fn.FileFilterFunction;
import org.vincentg.spark.fn.HiveRecMapper;
import org.vincentg.spark.fn.VoidFileWriterFunction;
import org.json.simple.JSONObject;

import kafka.serializer.StringDecoder;

/***
*
* @author vincentg
* @since 03-28-2016
*
*/
public class StreamIngestionApp implements Serializable {

	private static final long serialVersionUID = 2313473322214547133L;

	private MQIUtil mqiUtil = null;

	private EnumOuputType outputType = null;
	private EnumRecType recType = null;
	private EnumInputType inputType = null;

	private JSONObject appParams = null;

	HashMap<String,String> recSchema = null;

	public StreamIngestionApp(JSONObject arg) {
		this.mqiUtil = MQIUtil.getInstance();
		this.appParams = arg;

		try {
			this.recType = mqiUtil.getRecType(arg);
			this.inputType = mqiUtil.getInputType(arg);
			this.outputType = mqiUtil.getOuputType(arg);
			this.recSchema = mqiUtil.getRecSchema(arg);
		} catch (Exception e) {
			throw(e);
		}
	}

	public void run() {

		try {

			SparkConf spkconf = new SparkConf();
			spkconf.setMaster(MQIConstants.SPARK_MASTER);
			spkconf.setAppName( (String)appParams.get("app_name") );
			spkconf.set("spark.streaming.stopGracefullyOnShutdown","true");
			spkconf.set("spark.streaming.fileStream.minRememberDuration", "31557600s");

			spkconf.set("spark.sql.hive.metastore.version", "1.2.1");
			spkconf.set("spark.sql.hive.metastore.jars", "maven");

			final JavaStreamingContext jsctx =
					new JavaStreamingContext(
							spkconf,
							Durations.seconds(10));

			//MESSAGE STREAM FROM MESSAGE QUEUE (KAFKA, RABBITMQ, ETC.)
			JavaPairInputDStream<String, String> messages = null;


			//KAFKA
			if ( inputType == EnumInputType.KAFKA ) {

				HashSet<String> topicsSet =
						mqiUtil.extractKafkaTopics(
								(String)appParams.get("input"));

				HashMap<String, String> kafkaParams = new HashMap<>();

				kafkaParams.put(
						"metadata.broker.list",
						mqiUtil.getInputLoc(appParams, inputType));

				messages = KafkaUtils.createDirectStream(
								jsctx,
								String.class,
								String.class,
								StringDecoder.class,
								StringDecoder.class,
								kafkaParams,
								topicsSet);
			//FILESYSTEM
			} else if ( inputType == EnumInputType.FILESYSTEM ) {
				messages = jsctx.fileStream(
						(String) appParams.get("input"),
						String.class,
						String.class,
						MQIFileInputFormat.class,
						new FileFilterFunction(),
						false);

			//RABBIT_MQ
			} else if ( inputType == EnumInputType.RABBIT_MQ ) {
				//TODO: Implement Rabbit MQ
				jsctx.close();
				return;
			} else {
				jsctx.close();
				return;
			}

//			messages.print(); //MQ Messages

			JavaPairDStream<String, HashMap<String,String>> records = null;

			RecordProcessorFactory rFactory = new RecordProcessorFactory();
			RecordProcessor recProc = rFactory.getRecordProcessor(recType);
			records = recProc.process(messages, recType, appParams);

//			records.print(); //Records

///*
			//WRITE RECORDS
			if (outputType == EnumOuputType.HIVE) {

				//connect to hive using JDBC
				ArrayList<String> hschema = this.mqiUtil.getHiveSchemaJdbc(
						(String) this.appParams.get("output"));

				//Connect to hive using console
//				ArrayList<String> hschema = mqiUtil.getHiveSchemaCmd(
//						(String) this.appParams.get("output"));

				JavaPairDStream<String, String> hiveRecs =
						records.mapToPair(new HiveRecMapper(hschema, recSchema));

//				System.out.println("hive records:");
//				hiveRecs.print(); //Hive-formatted Records

//				String outputLoc = "./output/blues_tbl"; //DEBUG
				String outputLoc = MQIConstants.HDFS_DEFAULT_FS
						+MQIConstants.DEFAULT_HIVE_WAREHOUSE +"/"
						+appParams.get("output");

				System.out.println("outputLoc => "+outputLoc);
				hiveRecs.foreachRDD( new VoidFileWriterFunction(outputLoc) );
			}

			//CATCH TERM SIG AND SHUTDOWN GRACEFULLY
			Runtime.getRuntime().addShutdownHook(new MQIShutdownHook(jsctx));
//*/
			jsctx.start();
			jsctx.awaitTermination();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
