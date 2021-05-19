package com.lucinda;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

class KafkaService<T> implements Closeable {
	
	private final KafkaConsumer<String, T> consumer;
	private final ConsumerFunction parse;
	private Class<T> type;
	private Map<String, String> props;

	KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> props) {
		this.parse = parse;
		this.type = type;
		this.props = props;
		this.consumer = new KafkaConsumer<String, T>(properties(type, groupId, props));
		consumer.subscribe(Collections.singletonList(topic));
		
	}

	KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> props) {
		this.parse = parse;
		this.type = type;
		this.props = props;
		this.consumer = new KafkaConsumer<String, T>(properties(type, groupId, props));
		consumer.subscribe(topic);
	}

	void run() {
		while(true) {
			var records = consumer.poll(Duration.ofMillis(100));
			if(!records.isEmpty()){
				System.out.println("Found" + records.count() + "registers");
				for(var record : records) {
					try {
						parse.consume(record);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	private Properties properties(Class<T> type, String groupId, Map<String, String> props) {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
		properties.putAll(props);
		return properties;
	}

	@Override
	public void close() {
		consumer.close();
	}
}
