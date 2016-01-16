package com.teakdata.spark;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * An example of spark processing where a worker dynamically reads configuration from a file (/tmp/filters.cfg)
 * When the config files contains the word enabled, the processing applies a multiplication operation on values.
 * While running, update the file /tmp/filters.cfg (add or remove the enabled word) and see the processing 
 * is adapting accordingly (values are *10 or not)
 * 
 * @author smarcu
 */
class SparkStreamingExample {

	public static void main(String s[]) {
		StreamNumberServer.startNumberGeneratorServer(9999);

		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ConfigurableFilterApp");
		try (JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1))) {
			
			
			JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
			
			JavaDStream<SensorData> values = lines.map(line -> SensorData.fromString(line));
			
			values = values.map(new CfgFunction());
			
			values.print();
			
			jssc.start();              // Start the computation
			jssc.awaitTermination();   // Wait for the computation to terminate
		} 
	}
	
	
	/**
	 * Config function
	 *
	 */
	public static class CfgFunction implements Function<SensorData, SensorData> {

		@Override
		public SensorData call(SensorData v1) throws Exception {
			// create a new instance each call, evaluation is done on worker thread
			FilterConfig cfg = new FilterConfig();
			if (cfg.isFilterEnabled()) {
				v1.setData(v1.getData()*10);
			}	
			return v1;
		}
		
	}
	
	/**
	 * Config code
	 */
	public static class FilterConfig {
		public boolean isFilterEnabled() {
			try {
				String filterCfg = FileUtils.readFileToString(new File("/tmp/filters.cfg"));
				return filterCfg.contains("enabled");
			} catch (Exception e) {
				System.out.println("File not found");
			}
			return false;
		}
	}
	
}
