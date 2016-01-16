package com.teakdata.spark;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import org.junit.Before;
import org.junit.Test;

public class StreamNumberServerTest {
	
	private StreamNumberServer subject;

	@Before
	public void setUp() throws Exception {
		subject = new StreamNumberServer();
	}

	@Test
	public void test() {
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		PrintWriter out = new PrintWriter(bos, true);
		
		subject.stream(3, 0, 3, out);
		
		assertEquals("(A,0)\n(B,0)\n(C,0)\n(A,1)\n(B,1)\n(C,1)\n(A,2)\n(B,2)\n(C,2)\n", bos.toString());
	}

}
