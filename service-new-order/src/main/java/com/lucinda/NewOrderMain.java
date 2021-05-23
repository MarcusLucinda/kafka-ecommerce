package com.lucinda;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws ExecutionException, InterruptedException {

		try(var orderDispatcher = new KafkaDispatcher<Order>()){
			try(var emailDispatcher = new KafkaDispatcher<String>()){
				var userId = UUID.randomUUID().toString();
				var amount = new BigDecimal(Math.random() * 5000 + 1);
				var email = Math.random() + "@email.com";
				
				var order = new Order(userId, amount, email);
				orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderMain.class.getSimpleName()), order);
				var emailCode = "We're processing your order";
				emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, new CorrelationId(NewOrderMain.class.getSimpleName()), emailCode);
			}
		}
	}
}
