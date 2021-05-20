package com.lucinda;

import java.math.BigDecimal;

public class Order {

	private final String orderId, email;
	private final BigDecimal amount;
	
	public Order(String orderId, BigDecimal amount, String email) {
		super();
		this.orderId = orderId;
		this.amount = amount;
		this.email = email;
	}
	
	
	
}
