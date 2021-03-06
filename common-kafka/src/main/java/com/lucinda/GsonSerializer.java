package com.lucinda;

import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonSerializer<T> implements Serializer<T> {

	private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();
	
	/**
	 * serialize the message into bytes
	 */
	@Override
	public byte[] serialize(String topic, T data) {
		var serial = gson.toJson(data).getBytes();
		return serial;
	}
	
}	
