package com.lucinda;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonDeserializer<TMessage> implements Deserializer<Message>{
	
	private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();

	@Override
	public Message deserialize(String topic, byte[] data) {
		var serial = gson.fromJson(new String(data), Message.class);
		return serial;
	}
	

}
