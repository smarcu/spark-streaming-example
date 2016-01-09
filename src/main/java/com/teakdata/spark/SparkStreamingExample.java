package com.teakdata.spark;

import java.io.File;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.stream.IntStream;

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
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ConfigurableFilterApp");
		try (JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1))) {
			
			startNumberGeneratorServer(9999);
			
			JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
			
			JavaDStream<Integer> values = lines.flatMap(line -> Arrays.asList(line.split(" "))).map(strValue -> Integer.parseInt(strValue));
			
			values = values.map(new CfgFunction());
			
			values.print();
			
			jssc.start();              // Start the computation
			jssc.awaitTermination();   // Wait for the computation to terminate
		} 
	}
	
	/**
	 * Starts a number server, writes 0-254 numbers separated by spaces, after 254 writes a new line.
	 * @param port
	 */
	public static void startNumberGeneratorServer(int port) {
		Runnable serverThread = new Runnable() {
			public void run() {
				try (ServerSocket serverSocket = new ServerSocket(port)) {
					Socket clientSocket = serverSocket.accept();
					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
					while (true) {
						try {
							IntStream.range(0,  254).forEach(val -> out.print(val + " "));
							out.println("");
							Thread.sleep(500);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		new Thread(serverThread).start();
	}
	
	
	/**
	 * Config function
	 *
	 */
	public static class CfgFunction implements Function<Integer, Integer> {

		@Override
		public Integer call(Integer v1) throws Exception {
			// create a new instance each call, evaluation is done on worker thread
			FilterConfig cfg = new FilterConfig();
			return cfg.isFilterEnabled() ? v1*10 : v1;
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
