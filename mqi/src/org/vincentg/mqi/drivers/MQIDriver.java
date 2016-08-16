package org.vincentg.mqi.drivers;

import java.io.BufferedReader;
import java.io.FileReader;

import org.vincentg.spark.apps.RdbIngestionApp;
import org.vincentg.spark.apps.StreamIngestionApp;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


/***
*
* @author vincentg
* @since 03-28-2016
*
*/
public class MQIDriver {

	public static void main(String[] args) {

		try {

//			args[0] = "./tmp/test_db.json"; //debug
			args[0] = "./tmp/test_fs.json"; //debug

			JSONObject jo = retrieveParams(args[0]);

			System.out.println("user        :"+jo.get("user"));
			System.out.println("app_name    :"+jo.get("app_name"));
			System.out.println("input       :"+jo.get("input"));
			System.out.println("input type  :"+jo.get("input_type"));
			System.out.println("input_loc   :"+jo.get("input_loc"));
			System.out.println("rec_type    :"+jo.get("rec_type"));
			System.out.println("delimiter   :"+jo.get("delimiter"));
			System.out.println("output      :"+jo.get("output"));
			System.out.println("output_type :"+jo.get("output_type"));
			System.out.println("output_loc  :"+jo.get("output_loc"));
			System.out.println("template    :"+jo.get("template").toString());

			//TODO: Implement better checking for launching streaming and batch APPS
			if (jo.get("input_type").toString().contentEquals("rdbms")) {
				RdbIngestionApp rdbia = new RdbIngestionApp(jo);
				rdbia.run();
			} else {
				StreamIngestionApp kia = new StreamIngestionApp(jo);
				kia.run();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static JSONObject retrieveParams(String pfile) {
		JSONObject paramj = null;

		try {

			JSONParser jp = new JSONParser();
			paramj = (JSONObject) jp.parse(
					new BufferedReader(new FileReader(pfile)));

		} catch (Exception e) {
			e.printStackTrace();
		}
		return paramj;
	}

}
