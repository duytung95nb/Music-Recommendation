package MusicRecommendation;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import CassandraDatabase.Connector;
import CassandraDatabase.QueryExecute;
import shapeless.newtype;

public class Main {

	public static void main(String[] args) {
//		String pathDatabase  = "/home/danh/SharedFolder/database_test.csv";
//		
//		SparkConf sparkConf = new SparkConf().setAppName("Similarity");
//		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
//		
//		// Get all musics
//		JavaRDD<String> musicsInLines = sparkContext.textFile(pathDatabase);
//		System.out.println("========> count: " + musicsInLines.count());
//		JavaRDD<Map<String, Double>> TFs = musicsInLines.map(new ParseDatabaseToTFMap());
//		for ( Map<String, Double> TF : TFs.collect()) {
//			for (String key : TF.keySet()) {
//			    System.out.println(key + " : " + TF.get(key));
//			}
//			System.out.println("=====================Hi===============================");
//			System.out.println("=====================Hi2===============================");
//			System.out.println("=====================Hi3===============================");
//			System.out.println("======================================================");
//		}
//		
//		sparkContext.close();
//		sparkContext.stop();
		Connector connector = new Connector(); 
		QueryExecute execute = new QueryExecute(connector);
		ResultSet result = execute.selectAll("modies");
		System.out.println("Du lieu bang modies la:");
		for (Row row : result) {
			System.out.println(row);
		}
		List<Entry<String,String>> testRow = new ArrayList();
		SimpleEntry<String, String> entry1 = new SimpleEntry("id", "4");
		SimpleEntry<String, String> entry2 = new SimpleEntry("director", "Dao Tung");
		SimpleEntry<String, String> entry3 = new SimpleEntry("title", "Teo em");
		SimpleEntry<String, String> entry4 = new SimpleEntry("year", "1995");
		testRow.add(entry1);
		testRow.add(entry2);
		testRow.add(entry3);
		testRow.add(entry4);
		ResultSet insertResult = execute.insert_text_value("modies", testRow);
		System.out.println(insertResult);
	}

}
