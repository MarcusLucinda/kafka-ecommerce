package com.lucinda;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

class KafkaDispatcher<T> implements Closeable{

	private final KafkaProducer<String, Message<T>> producer;

	public KafkaDispatcher() {
		this.producer = new KafkaProducer<String, Message<T>>(properties());
	}

	
	/**
	 * send the message for consumpion
	 * @param topic | the topic the record will be appended to
	 * @param key | the key that will be included in the record
	 * @param id | the message correlation id
	 * @param payload | the message data
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	void send(String topic, String key, CorrelationId id, T payload) throws InterruptedException, ExecutionException {
		var value = new Message<T>(id, payload);
		var record = new ProducerRecord<>(topic, key, value);
        Callback callback = (data, ex) -> {
            if(ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Success enviando " + data.topic() + ":::partition " + data.partition() + "/ offset "  + data.offset() + "/ timestamp " + data.timestamp());
        };
		producer.send(record, callback).get();
	}
	
	private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

	@Override
	public void close() {
		producer.close();
	}

}
