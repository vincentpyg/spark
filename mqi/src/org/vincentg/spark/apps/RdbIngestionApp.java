package org.vincentg.spark.apps;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.vincentg.hadoop.io.MQIBatchFileOutputFormat;
import org.vincentg.mqi.constants.MQIConstants;
import org.vincentg.mqi.enums.EnumOuputType;
import org.vincentg.mqi.enums.EnumRecType;
import org.vincentg.mqi.util.MQIUtil;
import org.vincentg.spark.fn.HiveRecMapper;
import org.vincentg.spark.fn.RowRecordMapper;
import org.json.simple.JSONObject;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class RdbIngestionApp implements Serializable {

	private static final long serialVersionUID = 2313473322214547133L;

	private MQIUtil mqiUtil = null;

	private EnumOuputType outputType = null;
	private EnumRecType recType = null;

	private JSONObject appParams = null;

	HashMap<String,String> recSchema = null;

	public RdbIngestionApp(JSONObject arg) {
		this.mqiUtil = MQIUtil.getInstance();
		this.appParams = arg;

		try {
			this.recType = mqiUtil.getRecType(arg);
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

			JavaSparkContext jsctx = new JavaSparkContext(spkconf);
			SQLContext sqlctx = new SQLContext(jsctx);

			// MySQL
			HashMap<String,String> options = new HashMap<>();

			if (recType == EnumRecType.MYSQL) {
				options.put("driver", "com.mysql.jdbc.Driver");
				options.put("dbtable", (String) appParams.get("input"));
				options.put("url", (String) appParams.get("input_loc"));
			} else {
				jsctx.close();
				return;
			}

			DataFrame df = sqlctx.read().format("jdbc").options(options).load();
			String[] schema = df.columns();

			JavaRDD<Row> rows = df.toJavaRDD();
			JavaPairRDD<String,HashMap<String,String>> records = rows
					.mapToPair(new RowRecordMapper(
							schema,
							options.get("dbtable")));

			if (outputType == EnumOuputType.HIVE) {

//				//connect to hive using JDBC
				ArrayList<String> hschema = this.mqiUtil.getHiveSchemaJdbc(
						(String) this.appParams.get("output"));

				//Connect to hive using console
//				ArrayList<String> hschema = mqiUtil.getHiveSchemaCmd(
//						(String) this.appParams.get("output"));

				JavaPairRDD<String, String> hiveRecs =
						records.mapToPair(new HiveRecMapper(hschema, recSchema));


//				String outputLoc = "./output/blues_tbl"; //DEBUG
				String outputLoc = MQIConstants.HDFS_DEFAULT_FS
						+MQIConstants.DEFAULT_HIVE_WAREHOUSE +"/"
						+appParams.get("output");

				System.out.println("outputLoc => "+outputLoc);

				hiveRecs.saveAsNewAPIHadoopFile(
						outputLoc,
						Text.class,
						Text.class,
						MQIBatchFileOutputFormat.class);
			}


			jsctx.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

