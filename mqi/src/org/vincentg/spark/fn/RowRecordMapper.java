package org.vincentg.spark.fn;

import java.util.HashMap;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class RowRecordMapper implements PairFunction<Row,String,HashMap<String,String>> {

	private static final long serialVersionUID = 1L;

	private String[] schema = null;
	private String tableName = null;

	public RowRecordMapper(String[] schema, String tableName) {
		this.schema = schema;
		this.tableName = tableName;
	}

	@Override
	public Tuple2<String, HashMap<String, String>> call(Row row)
			throws Exception {

		HashMap<String,String> rowMap = new HashMap<>();

		for (String field : schema) {
			rowMap.put(field, row.get(row.fieldIndex(field)).toString() );
		}

		return new Tuple2<String, HashMap<String,String>>(tableName,rowMap);
	}
}
