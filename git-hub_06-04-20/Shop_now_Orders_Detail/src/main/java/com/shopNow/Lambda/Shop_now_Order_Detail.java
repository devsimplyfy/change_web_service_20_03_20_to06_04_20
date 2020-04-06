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

public class Shop_now_Order_Detail implements RequestHandler<JSONObject, JSONObject> {

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
		if (!input.containsKey("product_id")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'product_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("order_id")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'order_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("page_number")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'page_number' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}

		JSONObject jo_CartItem_Result_final = new JSONObject();
		JSONObject orderBillingAddress = new JSONObject();
		JSONObject orderShippingAddress = new JSONObject();

		JSONArray json_array_orderItem = new JSONArray();
		JSONArray json_array_orderShipingAddress = new JSONArray();
		JSONArray json_array_orderShipingAddress1 = new JSONArray();
		JSONArray json_array_orderBillingAddress = new JSONArray();
		JSONArray json_array_orderShippingAddress = new JSONArray();
		JSONArray jo_promocode_array = new JSONArray();
		JSONArray json_array_vendor = new JSONArray();

		Object userid1 = input.get("userid");
		Object product_id1 = input.get("product_id").toString();
		Object order_id1 = input.get("order_id");
		String order_id = order_id1.toString();
		long userid;
		long product_id;

		// String order_status = input.get("order_status").toString();

		String Str_msg;
		int page_number = 1;
		String order1;
		int flagChange;
		float total = 0;
		String Order_Id = null;
		String delivery_status_code = null;
		String payment_mode = null;
		String payment_status = null;
		java.sql.Date expected_delivery_Date = null;
		String transaction_id = null;
		int billing_address_id = 0;
		int shipping_address_id = 0;
		java.sql.Date orderDate = null;
		String Order_Number = null, promocode_value = null;

		float grand_total = 0, sub_total = 0, tax = 0, shipping = 0;
		String promocode = null;

		int page_size = 25;
		float Product_total = 0, discount = 0;
		String Vendor_id = null;
		String order_status = null;
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

		if (userid1 == null || userid1 == "") {

			Str_msg = "UserID cannot be null";
			jo_CartItem_Result_final.put("status", "0");
			jo_CartItem_Result_final.put("message", Str_msg);
			return jo_CartItem_Result_final;
		} else {
			userid = Long.parseLong(userid1.toString());
		}

		if (product_id1 == null || product_id1 == "") {

			product_id = 0;
		} else {
			product_id = Long.parseLong(product_id1.toString());

		}
		if (order_id1 == null || order_id1 == "") {

			order_id = null;

			Str_msg = "OrderID cannot be null";
			jo_CartItem_Result_final.put("status", "0");
			jo_CartItem_Result_final.put("message", Str_msg);
			return jo_CartItem_Result_final;

		} else {
			order_id = order_id1.toString();

		}

		Properties prop = new Properties();

		try {
			prop.load(getClass().getResourceAsStream("/application.properties"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			logger.log("Caught exception: " + e1);
		}

		try {

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

			String sql1 = "SELECT id FROM customer_order_details where order_id='" + order_id + "'";
			Statement stmt_order = conn.createStatement();
			ResultSet srs_order_id = stmt_order.executeQuery(sql1);

			if (srs_order_id.next() == false) {
				Str_msg = "Order not found";
				jo_CartItem_Result_final.put("status", "0");
				jo_CartItem_Result_final.put("message", Str_msg);
				return jo_CartItem_Result_final;

			}
			srs_order_id.close();
			stmt_order.close();

			String sql_vendor = "SELECT DISTINCT(vendor_id),order_id,admin.*  FROM customer_order_details LEFT JOIN admin ON customer_order_details.vendor_id=admin.id WHERE customer_order_details.order_id='"
					+ order_id + "'";
			Statement stmt_vendor = conn.createStatement();
			ResultSet srs_vendor = stmt_vendor.executeQuery(sql_vendor);

			while (srs_vendor.next()) {
				JSONObject jo_vendor = new JSONObject();
				jo_vendor.put("vendor_id", srs_vendor.getString("vendor_id"));
				jo_vendor.put("Vendor_name", srs_vendor.getString("name"));
				jo_vendor.put("contact_detail", srs_vendor.getString("emailid"));
				json_array_vendor.add(jo_vendor);
			}
			srs_vendor.close();
			stmt_vendor.close();

			String sql11 = null, sql_order_detail = null;

			if ((product_id == 0) && (order_id != null || order_id != "")) {

				sql_order_detail = "SELECT * FROM order_details INNER JOIN products ON products.id=order_details.productId WHERE order_details.order_id ='"
						+ order_id + "' and order_details.user_id ='" + userid + "' limit "
						+ (page_number - 1) * page_size + "," + page_size;

				sql11 = "SELECT table1.*,products.* FROM(SELECT cod.product_id,cod.quantity,cod.price,cod.delivery_status_code,cod.expected_date_of_delivery,customer_orders.* FROM customer_order_details as cod INNER JOIN customer_orders ON customer_orders.order_id=cod.order_id  WHERE customer_orders.order_id='"
						+ order_id + "' AND customer_orders.customer_id=" + userid
						+ " )AS table1 LEFT JOIN products ON table1.product_id=products.id limit "
						+ (page_number - 1) * page_size + "," + page_size;

				 logger.log("\n we are in if " + sql11);

			} else if (product_id != 0 && userid != 0 && order_id == null) {

				sql_order_detail = "SELECT * FROM order_details  INNER JOIN products ON products.id=order_details.productId WHERE  order_details.user_id ='"
						+ userid + "' and order_details.productId='" + product_id + "'";

				sql11 = "SELECT table1.*,products.* FROM(SELECT cod.product_id,cod.quantity,cod.price,cod.delivery_status_code,cod.expected_date_of_delivery,customer_orders.* FROM customer_order_details as cod INNER JOIN customer_orders ON customer_orders.order_id=cod.order_id  WHERE customer_orders.order_id='"
						+ order_id + "' AND customer_orders.customer_id=" + userid + " AND cod.product_id='"
						+ product_id + "' )AS table1 LEFT JOIN products ON table1.product_id=products.id";

				 logger.log("\n we are in else if " + sql11);

				Statement stmt1 = conn.createStatement();
				ResultSet str_order_product = stmt1.executeQuery(sql11);

				if (str_order_product.wasNull()) {

					jo_CartItem_Result_final.put("products", "No product found");
					jo_CartItem_Result_final.put("status", "0");
					return jo_CartItem_Result_final;

				}

				while (str_order_product.next()) {

					JSONObject jo_OrderItem1 = new JSONObject();

					jo_OrderItem1.put("product_id", str_order_product.getLong("product_id"));
					jo_OrderItem1.put("product_name", str_order_product.getString("product_description"));
					jo_OrderItem1.put("vendor_id", str_order_product.getString("vendor_id"));

					jo_OrderItem1.put("Image", str_order_product.getString("image"));
					float price = str_order_product.getFloat("price");
					jo_OrderItem1.put("price", price);
					jo_OrderItem1.put("quantity", str_order_product.getInt("quantity"));

					total = total + (price * str_order_product.getInt("quantity"));
					jo_OrderItem1.put("Product_total_price", price * str_order_product.getInt("quantity"));

					Order_Id = str_order_product.getString("order_id");
					Order_Number = str_order_product.getString("order_number");
					order_status = str_order_product.getString("order_status_code");
					
					//delivery_status_code = str_order_product.getString("delivery_status_code");
					//jo_OrderItem1.put("delivery_status_code", delivery_status_code);
					
					jo_OrderItem1.put("delivery_status_code", order_status);
					payment_mode = str_order_product.getString("mode_of_payment");
					payment_status = str_order_product.getString("payment_status");

					transaction_id = str_order_product.getString("transaction_id");
					billing_address_id = str_order_product.getInt("billing_address_id");
					shipping_address_id = str_order_product.getInt("delivery_address_id");

					grand_total = str_order_product.getFloat("grand_total");
					sub_total = str_order_product.getFloat("sub_total");
					tax = str_order_product.getFloat("tax");
					shipping = str_order_product.getFloat("shipping");
					discount = str_order_product.getFloat("discount");
					promocode = str_order_product.getString("promocode");

					orderDate = str_order_product.getDate("date_of_placed_order");
					expected_delivery_Date = str_order_product.getDate("expected_date_of_delivery");

					jo_OrderItem1.put("Order_Id", Order_Id);
					jo_OrderItem1.put("OrderNumber", Order_Number);
					jo_OrderItem1.put("Order_Date", orderDate);
					jo_OrderItem1.put("expected_delivery_Date", expected_delivery_Date);
					// jo_OrderItem1.put("Order_total_price", total);
					jo_OrderItem1.put("Order_status", order_status);
					jo_OrderItem1.put("payment_mode", payment_mode);
					jo_OrderItem1.put("payment_status", payment_status);
					jo_OrderItem1.put("transaction_id", transaction_id);
					// jo_OrderItem1.put("discount", discount);

					jo_CartItem_Result_final.put("Currency", "INR");
					json_array_orderItem.add(jo_OrderItem1);
					jo_CartItem_Result_final.put("Product_Detais", json_array_orderItem);

				}

			} else {

				/*
				 * sql11 =
				 * "SELECT * FROM order_details  INNER JOIN products ON products.id=order_details.productId WHERE order_details.order_id ='"
				 * + order_id + "' and order_details.user_id ='" + userid +
				 * "' and order_details.productId='" + product_id + "'";
				 * 
				 */
				sql11 = "SELECT table1.*,products.* FROM(SELECT cod.product_id,cod.quantity,cod.price,cod.delivery_status_code,cod.expected_date_of_delivery,customer_orders.* FROM customer_order_details as cod INNER JOIN customer_orders ON customer_orders.order_id=cod.order_id  WHERE customer_orders.order_id='"
						+ order_id + "' AND customer_orders.customer_id=" + userid + " AND cod.product_id='"
						+ product_id + "' )AS table1 LEFT JOIN products ON table1.product_id=products.id";

				 logger.log("\n we are in else " + sql11);

			}

			Statement stmt1 = conn.createStatement();
			ResultSet str_order_product = stmt1.executeQuery(sql11);

			while (str_order_product.next()) {

				JSONObject jo_OrderItem1 = new JSONObject();

				jo_OrderItem1.put("id", str_order_product.getLong("id"));
				jo_OrderItem1.put("product_id", str_order_product.getLong("product_id"));
				jo_OrderItem1.put("product_name", str_order_product.getString("name"));
				
				jo_OrderItem1.put("vendor_id", str_order_product.getString("vendor_id"));
				jo_OrderItem1.put("Image", str_order_product.getString("image"));
				float price = str_order_product.getFloat("price");
				jo_OrderItem1.put("price", price);
				jo_OrderItem1.put("quantity", str_order_product.getInt("quantity"));
				total = total + (price * str_order_product.getInt("quantity"));
				jo_OrderItem1.put("Product_total_price", price * str_order_product.getInt("quantity"));
				json_array_orderItem.add(jo_OrderItem1);
				Order_Id = str_order_product.getString("order_id");
				Order_Number = str_order_product.getString("order_number");
				delivery_status_code = str_order_product.getString("delivery_status_code");
				//jo_OrderItem1.put("delivery_status_code", delivery_status_code);
				
				payment_mode = str_order_product.getString("mode_of_payment");
				payment_status = str_order_product.getString("payment_status");
				transaction_id = str_order_product.getString("transaction_id");
				billing_address_id = str_order_product.getInt("delivery_address_id");
				shipping_address_id = str_order_product.getInt("delivery_address_id");
				orderDate = str_order_product.getDate("date_of_placed_order");
				expected_delivery_Date = str_order_product.getDate("expected_date_of_delivery");
				order_status = str_order_product.getString("order_status_code");
				grand_total = str_order_product.getFloat("grand_total");
				sub_total = str_order_product.getFloat("sub_total");
				tax = str_order_product.getFloat("tax");
				shipping = str_order_product.getFloat("shipping");
				discount = str_order_product.getFloat("discounts");
				promocode = str_order_product.getString("promocode");

			}
			 logger.log("\n i am here\n ");
			if (Order_Id == null) {

				jo_CartItem_Result_final.put("products", "No product found");
				jo_CartItem_Result_final.put("status", "0");
				return jo_CartItem_Result_final;

			}

			jo_CartItem_Result_final.put("Product_Details", json_array_orderItem);
			jo_CartItem_Result_final.put("Order_Id", Order_Id);
			jo_CartItem_Result_final.put("OrderNumber", Order_Number);
			jo_CartItem_Result_final.put("Order_Date", orderDate);
			jo_CartItem_Result_final.put("Order_Status", order_status);
			jo_CartItem_Result_final.put("expected_delivery_Date", expected_delivery_Date);
			jo_CartItem_Result_final.put("delivery_status_code", delivery_status_code);
			jo_CartItem_Result_final.put("payment_mode", payment_mode);
			jo_CartItem_Result_final.put("payment_status", payment_status);
			jo_CartItem_Result_final.put("transaction_id", transaction_id);
			jo_CartItem_Result_final.put("Currency", "INR");
			jo_CartItem_Result_final.put("Tax", tax);
			jo_CartItem_Result_final.put("Shipping", shipping);
		    jo_CartItem_Result_final.put("sub_total", sub_total);
		    jo_CartItem_Result_final.put("sub_total_before_tax", sub_total-discount);
			jo_CartItem_Result_final.put("Order_total_price", grand_total);
		    jo_CartItem_Result_final.put("discount", discount);
			jo_CartItem_Result_final.put("promocode", promocode);

			str_order_product.close();
			stmt1.close();

		} catch (Exception e) {

			logger.log("Caught exception: " + e);
		
		}

		try {
			if (!promocode.equalsIgnoreCase("null")) {
				Statement stmt_promocode = conn.createStatement();
				String promo_array[] = promocode.split(",", -2);

				for (int i = 0; i <= promo_array.length-1; i++) {
					
					//logger.log(promo_array[i]);
					String sql_promocode = "select * from promocodes where id='" + promo_array[i] + "'";
					ResultSet rs_promocode = stmt_promocode.executeQuery(sql_promocode);

					while (rs_promocode.next()) {
						JSONObject jo_promocode1 = new JSONObject();
						jo_promocode1.put("promocode", rs_promocode.getString("promocode"));
						jo_promocode1.put("description", rs_promocode.getString("description"));

						//logger.log("\n promocode \n");
						jo_promocode_array.add(jo_promocode1);
					}
				}
				
				stmt_promocode.close();

			}
		} catch (Exception e) {

			logger.log("Caught exception: " + e);
		}

		Statement stmt_billing_address = null;
		try {

			stmt_billing_address = conn.createStatement();
			String sql_billing_address = "select * from customer_order_address where id=" + billing_address_id;
			ResultSet rs_billing_address = stmt_billing_address.executeQuery(sql_billing_address);
			String first_name = null, last_name = null, address1 = null, address2 = null, address3 = null, city = null,
					state = null, country = null, email_address = null;
			int pincode = 0, phoneNumber = 0, delevery_address_id = 0;

			if (rs_billing_address.next()) {

				orderBillingAddress.put("firstName", rs_billing_address.getString("first_Name"));
				orderBillingAddress.put("lastName", rs_billing_address.getString("last_Name"));
				orderBillingAddress.put("address1", rs_billing_address.getString("address1"));
				orderBillingAddress.put("address2", rs_billing_address.getString("address2"));
				orderBillingAddress.put("address3", rs_billing_address.getString("address3"));
				orderBillingAddress.put("city", rs_billing_address.getString("city"));
				orderBillingAddress.put("state", rs_billing_address.getString("state"));
				orderBillingAddress.put("country", rs_billing_address.getString("country"));
				orderBillingAddress.put("email_address", rs_billing_address.getString("email_address"));
				orderBillingAddress.put("pincode", rs_billing_address.getInt("pincode"));
				orderBillingAddress.put("phoneNumber", rs_billing_address.getInt("phoneNumber"));

			}

			json_array_orderBillingAddress.add(orderBillingAddress);
			jo_CartItem_Result_final.put("billing_address", json_array_orderBillingAddress);
			stmt_billing_address.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.log("Caught exception: " + e);
		}

		Statement stmt_shipping_address = null;
		try {
			stmt_shipping_address = conn.createStatement();
			String sql_shipping_address = "select * from customer_order_address where id=" + shipping_address_id;
			ResultSet rs_billing_address = stmt_shipping_address.executeQuery(sql_shipping_address);
			String first_name = null, last_name = null, address1 = null, address2 = null, address3 = null, city = null,
					state = null, country = null, email_address = null;
			int pincode = 0, phoneNumber = 0, delevery_address_id = 0;

			if (rs_billing_address.next()) {

				// delevery_address_id=rs_billing_address.getInt("id");
				orderShippingAddress.put("firstName", rs_billing_address.getString("first_Name"));
				orderShippingAddress.put("lastName", rs_billing_address.getString("last_Name"));
				orderShippingAddress.put("address1", rs_billing_address.getString("address1"));
				orderShippingAddress.put("address2", rs_billing_address.getString("address2"));
				orderShippingAddress.put("address3", rs_billing_address.getString("address3"));
				orderShippingAddress.put("city", rs_billing_address.getString("city"));
				orderShippingAddress.put("state", rs_billing_address.getString("state"));
				orderShippingAddress.put("country", rs_billing_address.getString("country"));
				orderShippingAddress.put("email_address", rs_billing_address.getString("email_address"));
				orderShippingAddress.put("pincode", rs_billing_address.getInt("pincode"));
				orderShippingAddress.put("phoneNumber", rs_billing_address.getInt("phoneNumber"));

			}

			json_array_orderShippingAddress.add(orderShippingAddress);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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

		jo_CartItem_Result_final.put("Vendor", json_array_vendor);
		jo_CartItem_Result_final.put("promocode", jo_promocode_array);
		jo_CartItem_Result_final.put("shipping_address", json_array_orderShippingAddress);
		return jo_CartItem_Result_final;

	}
}
