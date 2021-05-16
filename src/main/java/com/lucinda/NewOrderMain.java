package com.lucinda;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws ExecutionException, InterruptedException {

		try(var dispatcher = new KafkaDispatcher()){
		var key = UUID.randomUUID().toString();
		var value = "132123,67523,7894589745";
		dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);
		var email = "We're processing your order";
		dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
		}
	}


}
