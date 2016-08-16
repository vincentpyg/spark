package org.vincentg.spark.fn;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/***
*
* @author vincentg
* @since 03-28-2016
*
*/
public class HiveRecMapper implements PairFunction< Tuple2<String,HashMap<String,String>>, String, String>{

	private static final long serialVersionUID = 5053246714331462681L;

	private ArrayList<String> hschema = null;
	private HashMap<String,String> rschema = null;

	public HiveRecMapper(
			ArrayList<String> hschema,
			HashMap<String,String> rschema) {

		this.hschema = hschema;
		this.rschema = rschema;

	}

	@Override
	public Tuple2<String, String> call(
			Tuple2<String, HashMap<String, String>> t) throws Exception {
		String hrec = "";

		if (hschema != null) {

			for (String field : this.hschema ) {

				//Get field_map
				String colNo = rschema.get(field);

				//Get value
				String value = colNo == null? "" : t._2().get(colNo);

				//create hive record
				hrec += value + "\t";
			}

		}

		return new Tuple2<String, String> (
				t._1,
				hrec.substring(0,hrec.length()-1) );
	}

}
