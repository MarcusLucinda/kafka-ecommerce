package com.lucinda;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;


public class CreateUserService {
	
	private Connection connection;

	
	/**
	 * connects to the db and create the users table if it not exists 
	 * @throws SQLException
	 */
	public CreateUserService() throws SQLException {
		String url = "jdbc:sqlite:target/users_database.db";
		this.connection = DriverManager.getConnection(url);
		connection.createStatement().execute("create table if not exists Users ("
				+ "uuid	varchar(200) primary key, "
				+ "email varchar(200))");
	}
	
	public static void main(String[] args) throws SQLException {
		var userService = new CreateUserService();
		try(var service = new KafkaService<>(CreateUserService.class.getSimpleName(), 
				"ECOMMERCE_NEW_ORDER", userService::parse, Map.of())){
			service.run();
		}
	}
	
	
	private void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException, SQLException {
		System.out.println("-----------------------------------");
		System.out.println("Processing new order: Checking for new user");
		System.out.println(record.key());
		System.out.println(record.value());
		var order = record.value().getPayload();
		if(isNewUser(order.getEmail())) {
			insertNewUser(order.getEmail());
		}
	}

	
	private void insertNewUser(String email) throws SQLException {
		var insert = connection.prepareStatement("insert into Users (uuid, email) values (?, ?)");
		String uuid = UUID.randomUUID().toString();
		insert.setString(1, uuid);
		insert.setString(2, email);
		insert.execute();
		System.out.println("User " + uuid + " " + email + " inserted");
		
	}

	private boolean isNewUser(String email) throws SQLException {
		var exists = connection.prepareStatement("select uuid from Users "
				+ "where email = ? limit 1");
		exists.setString(1, email);
		var result = exists.executeQuery();
		return !result.next();
	}
	
	
}
