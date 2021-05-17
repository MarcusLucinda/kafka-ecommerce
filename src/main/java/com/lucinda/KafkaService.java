package com.lucinda;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

class KafkaService implements Closeable {
	
	private final KafkaConsumer<String, String> consumer;
	private final ConsumerFunction parse;

	KafkaService(String groupId, String topic, ConsumerFunction parse) {
		this.parse = parse;
		this.consumer = new KafkaConsumer<String, String>(properties(groupId));
		consumer.subscribe(Collections.singletonList(topic));
		
	}

	KafkaService(String groupId, Pattern topic, ConsumerFunction parse) {
		this.parse = parse;
		this.consumer = new KafkaConsumer<String, String>(properties(groupId));
		consumer.subscribe(topic);
	}

	void run() {
		while(true) {
			var records = consumer.poll(Duration.ofMillis(100));
			if(!records.isEmpty()){
				System.out.println("Found" + records.count() + "registers");
				for(var record : records) {
					parse.consume(record);
				}
			}
		}
	}
	
	private static Properties properties(String groupId) {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		return properties;
	}

	@Override
	public void close() {
		consumer.close();
	}
}
