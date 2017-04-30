import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		String pathDatabase  = "/home/danh/SharedFolder/database_test.csv";
		
		SparkConf sparkConf = new SparkConf().setAppName("Similarity");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		// Get all musics
		JavaRDD<String> musicsInLines = sparkContext.textFile(pathDatabase);
		System.out.println("========> count: " + musicsInLines.count());
		JavaRDD<Map<String, Double>> TFs = musicsInLines.map(new ParseDatabaseToTFMap());
		for ( Map<String, Double> TF : TFs.collect()) {
			for (String key : TF.keySet()) {
			    System.out.println(key + " : " + TF.get(key));
			}
			System.out.println("======================================================");
		}
		
		sparkContext.close();
		sparkContext.stop();
	}

}
