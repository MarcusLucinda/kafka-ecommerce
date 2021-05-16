package com.lucinda;

import org.apache.kafka.clients.consumer.ConsumerRecord;


public class FraudDetectorService {

	public static void main(String[] args) {
		var fraudDetectorService = new FraudDetectorService();
		try(var service = new KafkaService(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", fraudDetectorService::parse)){
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, String> record) {
		System.out.println("-----------------------------------");
		System.out.println("Processing new order: Checking for fraud");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Order processed");
	}
}
