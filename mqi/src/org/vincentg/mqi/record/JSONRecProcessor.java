package org.vincentg.mqi.record;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.vincentg.mqi.enums.EnumRecType;
import org.vincentg.spark.fn.JSONRecMapper;
import org.json.simple.JSONObject;

public class JSONRecProcessor implements RecordProcessor, Serializable {

	private static final long serialVersionUID = 4055352423093636627L;

	@Override
	public JavaPairDStream<String, HashMap<String, String>> process(
			JavaPairInputDStream<String, String> messages, EnumRecType recType,
			JSONObject paramj) {

		return messages.mapToPair(new JSONRecMapper());
	}

}
