package com.lucinda;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;


public class ReportService {
	

	public static void main(String[] args) {
		var reportService = new ReportService();
		try(var service = new KafkaService<>(ReportService.class.getSimpleName(), 
				"ECOMMERCE_USER_GENERATE_REPORT", reportService::parse, User.class, Map.of())){
			service.run();
		}
	}
	

	private void parse(ConsumerRecord<String, Message<User>> record) {
		System.out.println("-----------------------------------");
		System.out.println("Processing report for " + record.value().getPayload());
	}

}
