package com.shopNow.Lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Shop_now_Order_Display implements RequestHandler<JSONObject, JSONObject> {

	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;

	@SuppressWarnings({ "unchecked", "unused" })
	public JSONObject handleRequest(JSONObject input, Context context) {

		LambdaLogger logger = context.getLogger();

		JSONObject errorPayload = new JSONObject();

		if (!input.containsKey("userid")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'userid' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("orderBy")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'orderBy' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("search")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'search' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("page_number")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'page_number' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("order_status")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'order_status' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("vendor_id")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'vendor_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}

		JSONObject jo_CartItem_Result_final = new JSONObject();
		JSONArray json_array_orderItem = new JSONArray();
		JSONArray jo_promocode_array = new JSONArray();

		Object userid1 = input.get("userid");
		long userid;
		String orderBy = input.get("orderBy").toString();
		String search;
		
		if (input.get("search") != null && input.get("search") != "") {
			search = input.get("search").toString();
		} else {

			search = "";
		}
		
		if(search.contains("'")) {
			
			search=search.replaceAll("'", "''");
		}
		
		
		
		String order_status = input.get("order_status").toString();
		String vendor_id1 = input.get("vendor_id").toString();
		String promocode_value = null;

		String vendor_id = null;
		float price = 0;

		//logger.log(vendor_id1);

		String Str_msg;
		int page_number = 1;
		String order1;
		int flagChange;
		Connection conn = null;
		// --------------------------------------------------------------------------------------------------

		if (input.get("page_number").toString() == "" || input.get("page_number").toString() == null) {
			page_number = 1;
		} else {
			String pagenumber1 = input.get("page_number").toString();
			page_number = Integer.parseInt(pagenumber1);
			if (page_number == 0) {
				page_number = 1;
			}
		}

		int page_size = 10;
		float Product_total = 0;

		if (userid1 == null || userid1 == "") {

			Str_msg = "UserID cannot be null";
			jo_CartItem_Result_final.put("status", "0");
			jo_CartItem_Result_final.put("message", Str_msg);
			return jo_CartItem_Result_final;
		} else {
			userid = Long.parseLong(userid1.toString());
		}

		if (order_status == null || order_status == "") {

			order_status = "";
		}

		if (orderBy == null) {
			order1 = "DESC";
			flagChange = 0;
		} else if (orderBy.equalsIgnoreCase("ASC")) {
			order1 = "ASC";
			flagChange = 1;
		} else if (orderBy.equalsIgnoreCase("DESC")) {
			order1 = "DESC";
			flagChange = 1;
		} else {
			order1 = "DESC";
			flagChange = 0;

		}

		try {

			Properties prop = new Properties();

			try {
				prop.load(getClass().getResourceAsStream("/application.properties"));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				logger.log("Caught exception: " + e1);
			}

			DB_URL = prop.getProperty("url");
			USERNAME = prop.getProperty("username");
			PASSWORD = prop.getProperty("password");
			conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);

		} catch (Exception e) {

			logger.log("Caught exception: " + e);
			JSONObject jo_catch = new JSONObject();
			jo_catch.put("Exception", e.getMessage());
			return jo_catch;
		}

		try {

			String sql = "SELECT id FROM customers where id='" + userid + "'";

			Statement stmt = conn.createStatement();
			ResultSet srs_customer_id = stmt.executeQuery(sql);

			if (srs_customer_id.next() == false) {
				Str_msg = "user is not valid";
				jo_CartItem_Result_final.put("status", "0");
				jo_CartItem_Result_final.put("message", Str_msg);
				return jo_CartItem_Result_final;

			}

			String sql2 = "select DISTINCT (order_id),delivery_status_code,vendorId,mode_of_payment,transaction_id,delivery_address,order_Date_Time,expected_date_of_delivery,payment_status,order_number from order_details where user_id ='"
					+ userid + "' and order_status='" + order_status + "'";

			String sql1;

			if (vendor_id1 == null || vendor_id1 == "") {
				sql1 = "select DISTINCT (order_id),product_description,delivery_status_code,vendorId,mode_of_payment,transaction_id,delivery_address,order_Date_Time,expected_date_of_delivery,payment_status,order_status,order_number,grand_total,sub_total,tax,shipping,promocode from order_details where user_id ='"
						+ userid + "' AND product_description LIKE '%" + search + "%' order by order_Date_Time "
						+ order1 + " limit " + (page_number - 1) * 10 + "," + page_size;

				sql1 = "SELECT DISTINCT (customer_orders.order_id),tax,sub_total,sub_total,order_status_code,date_of_placed_order,date_of_order_paid,payment_status,mode_of_payment,payment_gateway,transaction_id,authorization_id,shipping,grand_total,delivery_address_id,billing_address_id,promocode,discounts,order_number FROM(\r\n"
						+ "SELECT customer_order_details.id,customer_order_details.order_id,products.name,products.vendor_id FROM customer_order_details LEFT JOIN products ON customer_order_details.product_id=products.id )AS table1\r\n"
						+ "LEFT JOIN customer_orders ON table1.order_id=customer_orders.order_id where customer_id ='"
						+ userid + "' AND table1.name LIKE '%" + search + "%' and order_status_code like '%"
						+ order_status + "%' order by date_of_placed_order " + order1 + " limit "
						+ (page_number - 1) * 10 + "," + page_size;

			//	logger.log("\n we are in if:\n" + sql1);

			} else {

				sql1 = "select DISTINCT (order_id),product_description,delivery_status_code,vendorId,mode_of_payment,transaction_id,delivery_address,order_Date_Time,expected_date_of_delivery,payment_status,order_status,order_number,grand_total,sub_total,tax,shipping,promocode from order_details where user_id ='"
						+ userid + "' AND product_description LIKE '%" + search + "%' and order_details.vendorId='"
						+ vendor_id1 + "' order by order_Date_Time " + order1 + " limit " + (page_number - 1) * 10 + ","
						+ page_size;

				sql1 = "select * from customer_orders where customer_id ='" + userid
						+ "' order by date_of_placed_order " + order1 + " limit " + (page_number - 1) * 10 + ","
						+ page_size;

				sql1 = "select DISTINCT(cod.order_id),customer_orders.* FROM customer_orders LEFT JOIN customer_order_details AS cod ON cod.order_id=customer_orders.order_id where customer_orders.customer_id ='"
						+ userid + "' and cod.vendor_id='" + vendor_id1
						+ "' order by customer_orders.date_of_placed_order " + order1 + " limit "
						+ (page_number - 1) * 10 + "," + page_size;

				sql1 = "SELECT DISTINCT (customer_orders.order_id),tax,sub_total,sub_total,order_status_code,date_of_placed_order,date_of_order_paid,payment_status,mode_of_payment,payment_gateway,transaction_id,authorization_id,shipping,grand_total,delivery_address_id,billing_address_id,promocode,discounts,order_number FROM(\r\n"
						+ "SELECT customer_order_details.id, customer_order_details.order_id,products.name,products.vendor_id FROM customer_order_details LEFT JOIN products ON customer_order_details.product_id=products.id )AS table1\r\n"
						+ "LEFT JOIN customer_orders ON table1.order_id=customer_orders.order_id where customer_id ='"
						+ userid + "' AND table1.vendor_id='" + vendor_id1 + "' AND table1.name LIKE '%" + search
						+ "%' and order_status_code like '%" + order_status + "%' order by date_of_placed_order "
						+ order1 + " limit " + (page_number - 1) * 10 + "," + page_size;

				//logger.log("\n we are in else\n" + sql1);
			}

			Statement stmt1 = conn.createStatement();
			ResultSet str_order_ids = stmt1.executeQuery(sql1);

			while (str_order_ids.next()) {
				JSONArray json_array_orderItem1 = new JSONArray();
				JSONObject jo_CartItem_Result = new JSONObject();
				String sql3;

				if (vendor_id1 == null || vendor_id1 == "") {

					sql3 = "SELECT * FROM customer_order_details INNER JOIN products ON products.id=customer_order_details.product_id WHERE customer_order_details.order_id ='"
							+ str_order_ids.getString("order_id") + "'AND products.name LIKE '%" + search + "%'";

					// logger.log("\n we are in if2:\n" + sql3);
				} else {

					sql3 = "SELECT * FROM customer_order_details INNER JOIN products ON products.id=customer_order_details.product_id WHERE customer_order_details.order_id ='"
							+ str_order_ids.getString("order_id") + "' AND products.vendor_id='" + vendor_id1
							+ "' AND products.name LIKE '%" + search + "%'";

					// logger.log("\n we are in else2\n" + sql3);

				}

				Statement stmt3 = conn.createStatement();
				ResultSet str_order_product = stmt3.executeQuery(sql3);

				float total = 0;
				while (str_order_product.next()) {

					JSONObject jo_OrderItem1 = new JSONObject();
					jo_OrderItem1.put("product_id", str_order_product.getLong("product_id"));
					jo_OrderItem1.put("Image", str_order_product.getString("image"));
					jo_OrderItem1.put("product_name", str_order_product.getString("name"));
					jo_OrderItem1.put("vendor_id", str_order_product.getString("vendor_id"));
					jo_OrderItem1.put("quantity", str_order_product.getString("quantity"));

					vendor_id = str_order_product.getString("vendor_id");
					price = str_order_product.getFloat("price");

					jo_OrderItem1.put("Product_total_price", price * str_order_product.getInt("quantity"));
					jo_OrderItem1.put("expected_delivery_Date", str_order_product.getDate("expected_date_of_delivery"));
					json_array_orderItem1.add(jo_OrderItem1);

				}

				jo_CartItem_Result.put("Order_product", json_array_orderItem1);
				jo_CartItem_Result.put("Order_Id", str_order_ids.getString("order_id"));
				jo_CartItem_Result.put("OrderNumber", str_order_ids.getString("order_number"));
				jo_CartItem_Result.put("payment_mode", str_order_ids.getString("mode_of_payment"));
				jo_CartItem_Result.put("transaction_id", str_order_ids.getString("transaction_id"));
				jo_CartItem_Result.put("Order Date", str_order_ids.getDate("date_of_placed_order"));
				jo_CartItem_Result.put("Order_Status", str_order_ids.getString("order_status_code"));

				// jo_CartItem_Result.put("expected_delivery_Date",
				// str_order_ids.getDate("expected_date_of_delivery"));

				jo_CartItem_Result.put("Currency", "INR");
				jo_CartItem_Result.put("Order_total_price", str_order_ids.getFloat("grand_total"));
				 jo_CartItem_Result.put("sub_total", str_order_ids.getDouble("sub_total"));
				 jo_CartItem_Result.put("sub_total_before_tax", str_order_ids.getDouble("sub_total")-str_order_ids.getFloat("discounts"));
				 jo_CartItem_Result.put("tax", str_order_ids.getFloat("tax"));
				 jo_CartItem_Result.put("shipping", str_order_ids.getFloat("shipping"));
				// jo_CartItem_Result.put("promocode", str_order_ids.getString("promocode"));
				 jo_CartItem_Result.put("discount", str_order_ids.getString("discounts"));

				/*
				 * String product = jo_CartItem_Result.get("Order_product").toString();
				 * logger.log("\n" + product); if(product.equals("[]")) { JSONObject
				 * jo_CartItem_Result_final1=new JSONObject();
				 * jo_CartItem_Result_final1.put("orders", "product not found"); return
				 * jo_CartItem_Result_final1;
				 * 
				 * }
				 */

				json_array_orderItem.add(jo_CartItem_Result);

			}
			if (json_array_orderItem.isEmpty()) {

				jo_CartItem_Result_final.put("message", "No Orders Found");
				jo_CartItem_Result_final.put("status", "0");
				return jo_CartItem_Result_final;

			}

		} catch (Exception e) {

			logger.log("Caught exception: " + e);
			JSONObject jo_catch = new JSONObject();
			jo_catch.put("Exception", e.getMessage());
			return jo_catch;
		} finally {
			if (conn != null) {
				try {
					if (!conn.isClosed()) {

						conn.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
					logger.log("Caught exception: " + e);
					JSONObject jo_catch = new JSONObject();
					jo_catch.put("Exception", e.getMessage());
					return jo_catch;
				}
			}
		}

		jo_CartItem_Result_final.put("orders", json_array_orderItem);
		return jo_CartItem_Result_final;

	}
}
