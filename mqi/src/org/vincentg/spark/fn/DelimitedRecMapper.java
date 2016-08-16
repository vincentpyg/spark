package org.vincentg.spark.fn;

import java.util.HashMap;

import org.apache.spark.api.java.function.PairFunction;
import org.vincentg.mqi.enums.EnumRecType;

import scala.Tuple2;

/***
*
* @author vincentg
* @since 03-28-2016
*
*/
public class DelimitedRecMapper
		implements PairFunction<Tuple2<String,String>, String, HashMap<String,String>> {


	private static final long serialVersionUID = 8962950926136798189L;

	private String delimiter = null;

	public DelimitedRecMapper(EnumRecType rtype, String delimiter) {
		setDelimiter(rtype, delimiter);
	}

	@Override
	public Tuple2<String, HashMap<String, String>> call(
			Tuple2<String, String> t) throws Exception {

		return new Tuple2<String, HashMap<String, String>>(
				t._1,
				extractFromDelimitedFile(t._2));
	}

	private void setDelimiter(EnumRecType rtype, String delimiter) {

		if (rtype == EnumRecType.TSV) {
			this.delimiter = "\t";
		} else if (rtype == EnumRecType.CSV) {
			this.delimiter = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
		} else if (rtype == EnumRecType.PSV) {
			this.delimiter = "|";
		} else {
			if (delimiter == null || delimiter.isEmpty()) {
				//DEFAULT TO TSV
				this.delimiter = "\t";
			} else {
				//USE CUSTOM DELIMITER
				this.delimiter = delimiter;
			}
		}

	}


	private HashMap<String, String> extractFromDelimitedFile(String srec) {

		HashMap<String, String> recMap = new HashMap<>();
		String[] fields = srec.split(this.delimiter);
		for (int index = 0; index < fields.length; index++) {
			recMap.put("" + index, fields[index]);
		}
		return recMap;

	}

}
