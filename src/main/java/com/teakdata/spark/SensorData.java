package com.teakdata.spark;

import java.io.Serializable;

public class SensorData implements Serializable {
	
	private static final long serialVersionUID = 4031948815801896435L;
	private String sensorId;
	private int data;
	
	public String getSensorId() {
		return sensorId;
	}
	public void setSensorId(String sensorId) {
		this.sensorId = sensorId;
	}
	public int getData() {
		return data;
	}
	public void setData(int data) {
		this.data = data;
	}
	
	/**
	 * read data in format (ID,NUMBER)
	 * @param sdatastr
	 * @return
	 */
	public static SensorData fromString(String sdatastr) {
		SensorData sdata = new SensorData();
		sdatastr = sdatastr.replaceAll("\\(|\\)", "");
		String split[] = sdatastr.split(",");
		sdata.setSensorId(split[0]);
		sdata.setData(Integer.parseInt(split[1]));
		return sdata;
	}
	
	public String toString() {
		return "("+sensorId+","+data+")";
	}
}
