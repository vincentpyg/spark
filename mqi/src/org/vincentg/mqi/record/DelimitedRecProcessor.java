package org.vincentg.mqi.record;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.vincentg.mqi.enums.EnumRecType;
import org.vincentg.spark.fn.DelimitedRecMapper;
import org.json.simple.JSONObject;

public class DelimitedRecProcessor implements RecordProcessor, Serializable{

	private static final long serialVersionUID = -960381761846777537L;

	@Override
	public JavaPairDStream<String, HashMap<String, String>> process(
			JavaPairInputDStream<String, String> messages, EnumRecType recType,
			JSONObject paramj) {

		return messages.mapToPair(new DelimitedRecMapper(
				recType,
				(String) paramj.get("delimiter")));
	}

}
