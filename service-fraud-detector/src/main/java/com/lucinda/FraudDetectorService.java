package com.lucinda;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;


public class FraudDetectorService {

	public static void main(String[] args) {
		var fraudDetectorService = new FraudDetectorService();
		try(var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(), 
				"ECOMMERCE_NEW_ORDER", fraudDetectorService::parse, Map.of())){
			service.run();
		}
	}
	
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();

	private void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {
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
		var order = record.value().getPayload();
		if(isFraud(order)) {
			System.out.println("Order is a fraud!");
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), new CorrelationId(FraudDetectorService.class.getSimpleName()), order);
		}
		else {
			System.out.println("Order processed");
			orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), new CorrelationId(FraudDetectorService.class.getSimpleName()), order);
		}
	}

	private boolean isFraud(Order order) {
		return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
	}
}
