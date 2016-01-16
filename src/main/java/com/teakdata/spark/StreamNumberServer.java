package com.teakdata.spark;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.stream.IntStream;

public class StreamNumberServer {
	/**
	 * Starts a number server, writes pairs of letter-number.
	 * The letter identifies the stream id, the number is the stream sample value.
	 * E.g. (A,0) (B,0) (C,0) new line (A,1)...
	 *
	 * @param port
	 */
	public static void startNumberGeneratorServer(int port) {
		Runnable serverThread = new Runnable() {
			public void run() {
				try (ServerSocket serverSocket = new ServerSocket(port)) {
					StreamNumberServer server = new StreamNumberServer();
					Socket clientSocket = serverSocket.accept();
					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
					int batch=10, from=0, to=batch;
					while (from < 100) {
						try {
							server.stream(3, from, to, out);
							from = to;
							to = to+batch;
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
	
	public void stream(int nrOfStreams, int start, int stop, PrintWriter out) {
		try {
			
			IntStream.range(start, stop)
				.mapToObj (Integer::new)
				.flatMap (i -> IntStream.range('A', 'A'+nrOfStreams)
						.mapToObj(ch -> new Character((char)ch))
						.map(ch -> new String("("+ch+","+i+")")))
				.forEach(p -> out.println(p));
			out.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
