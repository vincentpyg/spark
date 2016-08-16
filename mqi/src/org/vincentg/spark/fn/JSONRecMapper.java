package org.vincentg.spark.fn;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFunction;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.Tuple2;

/***
*
* @author vincentg
* @since 03-28-2016
*
*/
public class JSONRecMapper
		implements PairFunction<Tuple2<String,String>, String, HashMap<String,String>> {

	private static final long serialVersionUID = -6093600355064641339L;

	@Override
	public Tuple2<String, HashMap<String, String>> call(
			Tuple2<String, String> t) throws Exception {


		JSONParser jp = new JSONParser(); // Not serializable

		HashMap<String, String> recMap = new HashMap<>();
		JSONObject jo = (JSONObject) jp.parse(t._2());

		Iterator<?> i = jo.keySet().iterator();

		while (i.hasNext()) {
			String fName = (String) i.next();
			recMap.put(fName, (String) jo.get(fName));
		}
		return new Tuple2<String,HashMap<String,String>>(t._1,recMap);
	}

}
