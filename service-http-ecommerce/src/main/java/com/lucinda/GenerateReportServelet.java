package com.lucinda;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class GenerateReportServelet extends HttpServlet {

	private final KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<>();

	@Override
	public void destroy() {
		super.destroy();
		batchDispatcher.close();
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException{
		try {
			batchDispatcher.send("SEND_MESSAGE_TO_ALL_USERS", "USER_GENERATE_REPORT", "USER_GENERATE_REPORT");
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		
		
		System.out.println("Generate reports for all users");
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.getWriter().println("Report request generated");
	}
}
