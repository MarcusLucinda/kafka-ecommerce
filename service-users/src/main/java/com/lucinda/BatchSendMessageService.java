package com.lucinda;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class BatchSendMessageService {
	
	private Connection connection;
	private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

	public BatchSendMessageService() throws SQLException {
		String url = "jdbc:sqlite:target/users_database.db";
		this.connection = DriverManager.getConnection(url);
		connection.createStatement().execute("create table if not exists Users ("
				+ "uuid	varchar(200) primary key, "
				+ "email varchar(200))");
	}
	
	public static void main(String[] args) throws SQLException {
		var batchService = new BatchSendMessageService();
		try(var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(), 
				"ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS", batchService::parse, Map.of())){
			service.run();
		}
	}
	
	private void parse(ConsumerRecord<String, Message<String>> record) throws InterruptedException, ExecutionException, SQLException {
		var message = record.value();
		System.out.println("-----------------------------------");
		System.out.println("Processing new batch");
		System.out.println("Topic: " + record.value());
		for(User user : getAllUsers()) {
			userDispatcher.send(message.getPayload(), user.getUuid(), message.getId().continueWith(BatchSendMessageService.class.getSimpleName()), user);
		}
	}

	private List<User> getAllUsers() throws SQLException {
		var results = connection.prepareStatement("select uuid from Users").executeQuery();
		List<User> users = new ArrayList<User>();
		while(results.next()) {
			users.add(new User(results.getString(1)));
		}
		return users;
	}
	
	
}
