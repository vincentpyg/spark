package org.vincentg.mqi.record;

import java.util.HashMap;

import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.vincentg.mqi.enums.EnumRecType;
import org.json.simple.JSONObject;

public interface RecordProcessor {

	public JavaPairDStream<String, HashMap<String,String>> process(
			JavaPairInputDStream<String, String> messages,
			EnumRecType recType,
			JSONObject paramj);

}
