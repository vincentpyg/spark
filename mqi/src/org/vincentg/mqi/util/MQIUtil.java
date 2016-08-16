package org.vincentg.mqi.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.vincentg.mqi.constants.MQIConstants;
import org.vincentg.mqi.enums.EnumInputType;
import org.vincentg.mqi.enums.EnumOuputType;
import org.vincentg.mqi.enums.EnumRecType;
import org.json.simple.JSONObject;

/***
*
* @author vincentg
* @since 03-28-2016
*
*/
public class MQIUtil implements Serializable{

	private static final long serialVersionUID = 1793598348381903573L;

	private static MQIUtil utilInstance = null;

	private final String driver = "org.apache.hive.jdbc.HiveDriver";

	public static MQIUtil getInstance() {
		return utilInstance == null? new MQIUtil() : utilInstance;
	}

	public MQIUtil() {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public HashSet<String> extractKafkaTopics(String topics) {
		return new HashSet<String>( Arrays.asList( topics.split(",") ) );
	}

	public HashMap<String,String> getRecSchema(JSONObject paramj) {
//		HashMap<String,String> recSchema = null;
		HashMap<String,String> recSchema = new HashMap<>();

		JSONObject tmplt = (JSONObject) paramj.get("template");
		Iterator<?> i = tmplt.keySet().iterator();
		while (i.hasNext()) {
			String key = (String) i.next();
			recSchema.put(key, (String)tmplt.get(key));
		}
		return recSchema;
	}

	public ArrayList<String> getHiveSchemaJdbc(String tableName) {

		ArrayList<String> hschema = new ArrayList<>();

		try {

			Connection conn = DriverManager.getConnection(MQIConstants.HIVE_CONNECTION_STRING, "", "");
			Statement stmt = conn.createStatement();
			ResultSet rset = stmt.executeQuery("describe "+ tableName);

			while (rset.next()) {
				hschema.add(rset.getString(1));
			}
			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return hschema;
	}

	public ArrayList<String> getHiveSchemaCmd(String tableName) {

		ArrayList<String> schema = new ArrayList<>();

		try {

			String cmd = "hive -e 'describe "+tableName+"' | awk '{print $1\"\\t\"$2}'";
			System.out.println(cmd);

			ProcessBuilder pb = new ProcessBuilder(
				"/bin/sh",
				"-c",
				cmd
			);

			Process p = pb.start();
			p.waitFor();

			BufferedReader br = new BufferedReader(
					new InputStreamReader(p.getInputStream()));

			String col = null;
			while ( (col = br.readLine()) != null) {
				String[] parts = col.split("\t");
				if (parts.length == 2) {
					schema.add(parts[0]);
				}
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return schema;
	}


//	public ArrayList<String> getHiveSchemaSpark(JavaSparkContext jsctx, String tableName) {
//
//		ArrayList<String> hschemaAr = new ArrayList<>();
//
//		jsctx.getConf().set("spark.sql.hive.metastore.version", "0.13.0");
//		jsctx.getConf().set("spark.sql.hive.metastore.jars", "maven");
//
//		HiveContext hivectx = new HiveContext(jsctx);
//		Row[] hschema = hivectx.sql("describe "+tableName).collect();
//
//		for (Row r : hschema) {
//
//			hschemaAr.add( (String)r.get(0) );
//		}
//
//		return hschemaAr;
//	}

	public EnumOuputType getOuputType(JSONObject params) {

		EnumOuputType result = EnumOuputType.UNDEFINED;
		String sinkType = (String) params.get("output_type");

		if ( sinkType.contentEquals("hive") ) {
			result = EnumOuputType.HIVE;
		}

		return result;
	}

	public EnumRecType getRecType(JSONObject params) {

		EnumRecType result = EnumRecType.UNDEFINED;
		String recType = (String) params.get("rec_type");

		if ( recType.contentEquals("tsv") ) {
			result = EnumRecType.TSV;
			System.out.println("tsv");
		} else if ( recType.contentEquals("csv") ) {
			result = EnumRecType.CSV;
			System.out.println("csv");
		} else if (recType.contentEquals("json")) {
			result = EnumRecType.JSON;
			System.out.println("json");
		} else if (recType.contentEquals("mysql")) {
			result = EnumRecType.MYSQL;
			System.out.println("mysql");
		}

		return result;
	}

	public EnumInputType getInputType(JSONObject params) {

		EnumInputType result = EnumInputType.UNDEFINED;
		String recType = (String) params.get("input_type");

		if ( recType.contentEquals("kafka") ) {
			result = EnumInputType.KAFKA;
		} else if (recType.contentEquals("filesystem")) {
			result = EnumInputType.FILESYSTEM;
		} else if (recType.contentEquals("rdbms")) {
			result = EnumInputType.RDBMS;
		}

		return result;
	}

	public String getInputLoc(JSONObject params, EnumInputType srcType) {

		String inputLoc = (String) params.get("input_loc");

		if ( inputLoc == null || inputLoc.isEmpty() ) {
			if (srcType == EnumInputType.KAFKA)
				inputLoc = MQIConstants.DEFAULT_KAFKA_BROKERS;
		}

		return inputLoc;
	}

	public String getOutputLoc(JSONObject params, EnumOuputType sinkType) {
		String outputLoc = (String) params.get("ouput_loc");

		if ( outputLoc == null || outputLoc.isEmpty() ) {
			if (sinkType == EnumOuputType.HIVE)
				outputLoc = MQIConstants.DEFAULT_HIVE_WAREHOUSE;
		}

		return outputLoc;
	}


}