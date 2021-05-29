package com.lucinda;

import java.util.UUID;

public class CorrelationId {

	private final String id;
	
	public CorrelationId(String title) { 
		this.id = title + "(" + UUID.randomUUID().toString() + ")";
	}

	/**
	 * creates a correlation id to identify the message
	 */
	@Override
	public String toString() {
		return "CorrelationId [id=" + id + "]";
	}
	
	/**
	 * concatenate a correlation id with the message topic
	 * @param title
	 * @return
	 */
	public CorrelationId continueWith(String title) {
		return new CorrelationId(this.id + " - " + title);
	}
	
	
}
