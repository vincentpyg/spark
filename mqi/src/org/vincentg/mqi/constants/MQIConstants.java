package org.vincentg.mqi.constants;

/***
* @author vincentg
* @since 03-28-2016
*/
public class MQIConstants {

	public static final String SPARK_MASTER = "local[3]"; //LOCAL
//	public static final String SPARK_MASTER = "yarn-cluster";
	public static final String DEFAULT_KAFKA_BROKERS = "192.168.187.10:9092";
	public static final String DEFAULT_HIVE_WAREHOUSE ="/apps/hive/warehouse";
	public static final String HDFS_DEFAULT_FS = "hdfs://192.168.187.10:8020";
	public static final String HIVE_CONNECTION_STRING = "jdbc:hive2://192.168.187.10:10000/default";
}
