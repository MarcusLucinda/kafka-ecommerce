package com.lucinda;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class NewOrderServlet extends HttpServlet {

	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
	private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

	@Override
	public void destroy() {
		super.destroy();
		orderDispatcher.close();
		emailDispatcher.close();
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		var userId = UUID.randomUUID().toString();
		var amount = new BigDecimal(req.getParameter("amount"));
		var email = req.getParameter("email");
		try {
			var order = new Order(userId, amount, email);
			orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), order);
			var emailCode = "We're processing your order";
			emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), emailCode);
			System.out.println("New order successfully sent");
			resp.setStatus(HttpServletResponse.SC_OK);
			resp.getWriter().println("New order successfully sent");
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
}
