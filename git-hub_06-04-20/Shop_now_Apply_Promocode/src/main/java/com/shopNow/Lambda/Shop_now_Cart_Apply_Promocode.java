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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Shop_now_Cart_Apply_Promocode implements RequestHandler<JSONObject, JSONObject> {
	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;

	@SuppressWarnings({ "unchecked", "unused" })
	public JSONObject handleRequest(JSONObject input, Context context) {

		LambdaLogger logger = context.getLogger();
		logger.log("Invoked JDBCSample.getCurrentTime");
		JSONObject errorPayload = new JSONObject();

		if (!input.containsKey("userid")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'userid' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("promocode")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'promocode' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("device_id")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'device_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}

		Object userid1 = input.get("userid");
		Object promocode1 = input.get("promocode");
		String promocode = promocode1.toString();
		Object device_id1 = input.get("device_id");
		String device_id = device_id1.toString();

		long userid;
		int promocode_id;
		long cart_id;
		Connection conn = null;

		String Str_msg;
		float promo_value = 0, Sub_total = 0, Sub_total1 = 0, total_promo_Discount = 0;
		float Grand_total = 0;
		JSONArray promocodes_array = new JSONArray();
		JSONObject jo_cartInsert = new JSONObject();
		
		
		
		
		
		
		
		

		if (userid1 == null || userid1 == "") {
			userid = 0;
		} else {

			userid = Long.parseLong(userid1.toString());
		}

		// Get time from DB server

		if (promocode == null || promocode == "") {

			Str_msg = "No Promocode Entered";
			jo_cartInsert.put("status", "0");
			jo_cartInsert.put("message", Str_msg);

			return jo_cartInsert;
		}

		if ((device_id1 == null || device_id1 == "") && userid == 0) {

			Str_msg = "Please Enter either UserId or Device_id";
			jo_cartInsert.put("status", "0");
			jo_cartInsert.put("message", Str_msg);
			return jo_cartInsert;
		}

		Properties prop = new Properties();

		try {
			prop.load(getClass().getResourceAsStream("/application.properties"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			logger.log("Exception" + e1.toString());
		}

		try {

			DB_URL = prop.getProperty("url");
			USERNAME = prop.getProperty("username");
			PASSWORD = prop.getProperty("password");
			float tax = 0;

			conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
			String sql_customer = null, sql_customer_sum = null, sql_promo_cart = null, sql3_insert = null;
			float shipping = 0;

			// shipping Charge

			String first_name = null, last_name = null, address1 = null, address2 = null, address3 = null,
					city = "city", state = null, country = null, email_address = null;
			int pincode = 0, delevery_address_id = 0;

			String city_buyer = null, state_buyer = null, country_buyer = null, zone = null;
			int is_metro = 0, is_special_destination = 0, is_RoI_A = 0, is_RoI_B = 0;

			String city_seller = null, zone_seller = null, state_seller = null, country_seller = null;
			int is_metro_seller = 0, is_special_destination_seller = 0, is_RoI_A_seller = 0, is_RoI_B_seller = 0;

			float shipping_charge = 0;
			int plan_id = 0, courier_id = 0;

			String courier_name = "BlueDart";
			String category_name = "Standard";
			Long phoneNumber;

			String sql_buyer = "SELECT pincodes.* FROM address INNER JOIN pincodes ON address.pincode=pincodes.pincode WHERE customerId='"
					+ userid + "' AND isPrimary=1";
			Statement stmt_customer_add = conn.createStatement();
			ResultSet srs_customer_add = stmt_customer_add.executeQuery(sql_buyer);

			if (srs_customer_add.next()) {

				city = srs_customer_add.getString("city");
				state = srs_customer_add.getString("state");
				country = srs_customer_add.getString("country");
				pincode = srs_customer_add.getInt("pincode");
				zone = srs_customer_add.getString("zone");
				is_metro = srs_customer_add.getInt("is_metro");
				is_special_destination = srs_customer_add.getInt("is_special_destination");
				is_RoI_A = srs_customer_add.getInt("is_RoI_A");
				is_RoI_B = srs_customer_add.getInt("is_RoI_B");

			} else {
				/*
				 * Str_msg = "Sorry, we currently do not deliver to your default address.";
				 * jsonObject_cartDisplay_result.put("status", "0");
				 * jsonObject_cartDisplay_result.put("message", Str_msg); return
				 * jsonObject_cartDisplay_result;
				 */
			}

			srs_customer_add.close();
			stmt_customer_add.close();

			if (city.equalsIgnoreCase("city") == false) {

				String sql_srs_vendor = null;
				if (userid != 0) {
					sql_srs_vendor = "SELECT DISTINCT (VendorId) FROM cart_items WHERE UserId='" + userid + "'";

					// logger.log("sql_srs_vendor\n" + sql_srs_vendor);

					Statement stmt_vendor_shipping = conn.createStatement();
					ResultSet srs_vendor = stmt_vendor_shipping.executeQuery(sql_srs_vendor);

					ArrayList<Integer> arrlist_vendor = new ArrayList<Integer>();

					int vendor_id_isexternal1 = 0;
					while (srs_vendor.next()) {

						vendor_id_isexternal1 = srs_vendor.getInt("VendorId");
						arrlist_vendor.add(vendor_id_isexternal1);

					}

					srs_vendor.close();
					stmt_vendor_shipping.close();

					int n = arrlist_vendor.size();

					for (int s = 0; s < n; s++) {

						int vendorId_charg = arrlist_vendor.get(s);
						String pincode_Sql_seller = "SELECT * FROM (SELECT * FROM vendor_address WHERE vendor_id ="
								+ vendorId_charg + " )AS table1 INNER JOIN pincodes ON pincodes.pincode=table1.pincode";

						// logger.log("\n pincode_Sql_seller \n" + pincode_Sql_seller);

						Statement stmt_pincode_seller = conn.createStatement();
						ResultSet resultSet_seller = stmt_pincode_seller.executeQuery(pincode_Sql_seller);
						JSONObject jsonObject_pincode_seller = new JSONObject();
						while (resultSet_seller.next()) {

							jsonObject_pincode_seller.put("id", resultSet_seller.getInt("id"));

							// vendor_id=resultSet_seller.getInt("vendor_id");

							city_seller = resultSet_seller.getString("city");
							zone_seller = resultSet_seller.getString("zone");
							state_seller = resultSet_seller.getString("state");
							country_seller = resultSet_seller.getString("country");
							is_metro_seller = resultSet_seller.getInt("is_metro");
							is_special_destination_seller = resultSet_seller.getInt("is_special_destination");
							is_RoI_A_seller = resultSet_seller.getInt("is_RoI_A");
							is_RoI_B_seller = resultSet_seller.getInt("is_RoI_A");

						}

						resultSet_seller.close();
						stmt_pincode_seller.close();

						String city_type = null;

						if (city.equalsIgnoreCase(city_seller)) {

							city_type = "intra_city";

						} else if (zone.equalsIgnoreCase(zone_seller)) {
							city_type = "intra_zone";

						} else {

							// logger.log("\n we are in else \n" + city_type);

							if ((is_metro & is_metro_seller) == 1) {

								city_type = "metro_to_metro";

							} else if (is_RoI_A == 1) {

								city_type = "roi_A";

							} else if (is_RoI_B == 1) {

								city_type = "roi_B";

							} else if (is_special_destination == 1) {

								city_type = "special_destination";

							} else {
								city_type = "null";

							}

						}

						// logger.log("\n city_type \n" + city_type);

						String sql_shipping = "SELECT shipping_categories.*,couriers.courier_name FROM couriers  INNER JOIN shipping_categories ON shipping_categories.courier_id=couriers.id WHERE courier_name='"
								+ courier_name + "' AND category_name='" + category_name
								+ "' AND weight='0-500 gms' AND vendor_id='" + vendorId_charg + "'";

						// logger.log("\n sql_shipping \n" + sql_shipping);

						Statement stmt_shipping = conn.createStatement();
						ResultSet sql_shipping1 = stmt_shipping.executeQuery(sql_shipping);

						if (sql_shipping1.next()) {

							shipping_charge = sql_shipping1.getFloat(city_type);
							plan_id = sql_shipping1.getInt("id");
							courier_id = sql_shipping1.getInt("courier_id");

						} else {

							shipping = 0;

						}

						sql_shipping1.close();
						stmt_shipping.close();

						shipping = shipping + shipping_charge;

					}
				} else {

					shipping = 0;

				}
			} else {

				shipping = 0;

			}

		
			
			
			
			
			
			
			
			
			
			
			
			
			String alredy_apply_promo=null;
			
			if (userid == 0) {
				sql_customer = "SELECT * FROM cart_items where device_id='" + device_id + "' AND UserId=0";

				sql_customer_sum = "SELECT SUM(cart_items.Quantity * p.sale_price) AS sub_total FROM products AS p LEFT JOIN cart_items ON p.id=cart_items.ProductId WHERE device_id='"
						+ device_id + "' AND UserId=0";

				sql_promo_cart = "SELECT * FROM promocodes LEFT JOIN cart_promocode ON promocodes.id=cart_promocode.promocode_id WHERE user_id=0 and device_id='"
						+ device_id+"'";
				alredy_apply_promo="SELECT * FROM cart_promocode where device_id='" + device_id+ "'AND user_id=0";
				

			} else {

				sql_customer = "SELECT * FROM cart_items where UserId=" + userid;

				sql_customer_sum = "SELECT SUM(cart_items.Quantity * p.sale_price) AS sub_total FROM products AS p LEFT JOIN cart_items ON p.id=cart_items.ProductId WHERE  UserId="
						+ userid;

				sql_promo_cart = "SELECT * FROM promocodes LEFT JOIN cart_promocode ON promocodes.id=cart_promocode.promocode_id WHERE user_id="
						+ userid;

			}
			
			// Logic for valid customer and device_id they have cart_items
			
			
			
			
			Statement stmt_customer = conn.createStatement();
			ResultSet resultSet_customer = stmt_customer.executeQuery(sql_customer);

			if (resultSet_customer.next() == false) {
				Str_msg = "Poduct  Not Found in Cart";
				jo_cartInsert.put("status", "0");
				jo_cartInsert.put("message", Str_msg);
				
				
				
				jo_cartInsert.put("promocodes_array", promocodes_array);
				jo_cartInsert.put("sub_total", Sub_total);
				jo_cartInsert.put("Discount", total_promo_Discount);
				jo_cartInsert.put("Tax", tax);
				jo_cartInsert.put("sub_total_before_tax", Sub_total - total_promo_Discount);
				jo_cartInsert.put("shipping", shipping);
				Grand_total = Sub_total + shipping + tax - total_promo_Discount;
				jo_cartInsert.put("Grand_total", Grand_total);
				
				return jo_cartInsert;

			}
			resultSet_customer.close();
			stmt_customer.close();

			
			
			
			//logic for find sum of cart_item_price
			
			logger.log("\n sql_customer_sum\n "+sql_customer_sum);
			
			Statement stmt_customer_sum = conn.createStatement();
			ResultSet resultSet_customer_sum = stmt_customer_sum.executeQuery(sql_customer_sum);

			if (resultSet_customer_sum.next()) {
				Sub_total = resultSet_customer_sum.getFloat("sub_total");
				Sub_total1 = Sub_total;

			}
			resultSet_customer_sum.close();
			stmt_customer_sum.close();

			
			logger.log("resultSet_customer_sum"+Sub_total);
			
		
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			//logic for already add apply promocode array
			
			Statement stmt_promo_cart = conn.createStatement();
			ResultSet resultSet_promo_cart = stmt_promo_cart.executeQuery(sql_promo_cart);

			while (resultSet_promo_cart.next()) {
				JSONObject promocodes = new JSONObject();

				promocodes.put("promocode_id", resultSet_promo_cart.getInt("id"));
				promocodes.put("promoname", resultSet_promo_cart.getString("description"));
				promocodes.put("promocode", resultSet_promo_cart.getString("promocode"));
				String type = resultSet_promo_cart.getString("promo_value_type");
			
				float promo_value1 = resultSet_promo_cart.getFloat("promo_value");
				if (type.equalsIgnoreCase("Variable")) {

					Sub_total1 = Sub_total1 - (Sub_total1 * promo_value1) / 100;

				} else if (type.equalsIgnoreCase("Fixed")) {

					Sub_total1 = Sub_total1 - promo_value1;
				}
				promocodes_array.add(promocodes);
				logger.log("\npromocodes_array  resultSet_promo_cart\n");
				
			}
			total_promo_Discount = Sub_total - Sub_total1;

			resultSet_promo_cart.close();
			stmt_promo_cart.close();

			float final_sub_total = Sub_total - total_promo_Discount;

			
			
			
			
					
					
			
			
			//Logic for get current apply promocode
			
			
			String promo_sql_promo = "SELECT * FROM promocodes where promocode='" + promocode + "' and status='1'";
			Statement stmt_promo = conn.createStatement();
			ResultSet resultSet_promo = stmt_promo.executeQuery(promo_sql_promo);

			float promo_dis_value = 0;
			promocode_id = 0;
			JSONObject promocodes_new = new JSONObject();
			
			
			
			
			
			if (resultSet_promo.next()) {

				String type = resultSet_promo.getString("promo_value_type");

				promocodes_new.put("promocode_id", resultSet_promo.getInt("id"));
				promocodes_new.put("promoname", resultSet_promo.getString("description"));
				promocodes_new.put("promocode", resultSet_promo.getString("promocode"));
				promocode_id = resultSet_promo.getInt("id");

				SimpleDateFormat f1 = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
				String s1 = null;
				Date d2, d3 = null;

				String dateStop = resultSet_promo.getString("end_time");
				String dateStop1 = dateStop.substring(0, dateStop.length() - 3);
				long dateStop2 = Long.parseLong(dateStop1);
				String edate = f1.format(new java.util.Date(dateStop2 * 1000));

				String current = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(Calendar.getInstance().getTime());

				d2 = f1.parse(edate);
				d3 = f1.parse(current);

				// in milliseconds
				long diff = d2.getTime() - d3.getTime();

				if (diff <= 0) {

					Str_msg = "Promocode  " + promocode + "  Expire";
					jo_cartInsert.put("status", "1");
					jo_cartInsert.put("message", Str_msg);

					jo_cartInsert.put("promocodes_array", promocodes_array);
					jo_cartInsert.put("sub_total", Sub_total);
					jo_cartInsert.put("Discount", total_promo_Discount);
					jo_cartInsert.put("Tax", tax);
					jo_cartInsert.put("sub_total_before_tax", Sub_total - total_promo_Discount);
					jo_cartInsert.put("shipping", shipping);
					Grand_total = Sub_total + shipping + tax - total_promo_Discount;
					jo_cartInsert.put("Grand_total", Grand_total);

					return jo_cartInsert;

				}

				float promo_value11 = resultSet_promo.getFloat("promo_value");
				if (type.equalsIgnoreCase("Variable")) {

					promo_dis_value = (Sub_total1 * promo_value11) / 100;

				} else if (type.equalsIgnoreCase("Fixed")) {

					promo_dis_value = promo_value11;
				}

			} else {

				Str_msg = "Invalid " + promocode + " Promocode entered";
				jo_cartInsert.put("status", "0");
				jo_cartInsert.put("message", Str_msg);

				jo_cartInsert.put("promocodes_array", promocodes_array);
				jo_cartInsert.put("sub_total", Sub_total);
				jo_cartInsert.put("Discount", total_promo_Discount);
				jo_cartInsert.put("Tax", tax);
				jo_cartInsert.put("sub_total_before_tax", Sub_total - total_promo_Discount);
				jo_cartInsert.put("shipping", shipping);
				Grand_total = Sub_total + shipping + tax - total_promo_Discount;
				jo_cartInsert.put("Grand_total", Grand_total);

				return jo_cartInsert;

			}

			resultSet_promo.close();
			stmt_promo.close();

			if (promo_dis_value > final_sub_total) {

				Str_msg = "Promocode  " + promocode + "  Not Applied Successfully";
				jo_cartInsert.put("status", "1");
				jo_cartInsert.put("message", Str_msg);

				jo_cartInsert.put("promocodes_array", promocodes_array);
				jo_cartInsert.put("sub_total", Sub_total);
				jo_cartInsert.put("Discount", total_promo_Discount);
				jo_cartInsert.put("Tax", tax);
				jo_cartInsert.put("sub_total_before_tax", Sub_total - total_promo_Discount);
				jo_cartInsert.put("shipping", shipping);
				Grand_total = Sub_total + shipping + tax - total_promo_Discount;
				jo_cartInsert.put("Grand_total", Grand_total);

				return jo_cartInsert;

			} else {

				if (userid == 0) {

					sql3_insert = "INSERT IGNORE INTO cart_promocode (device_id,user_id,promocode_id) VALUES('"
							+ device_id + "','" + userid + "','" + promocode_id + "')";

				} else {

					sql3_insert = "INSERT IGNORE INTO cart_promocode (user_id,promocode_id) VALUES('" + userid + "','"
							+ promocode_id + "')";

				}

				Statement stmt_insert = conn.createStatement();
				int i = stmt_insert.executeUpdate(sql3_insert);

				if (i > 0) {

					Str_msg = "Promocode  " + promocode + " Applied Successfully";
					jo_cartInsert.put("status", "1");
					jo_cartInsert.put("message", Str_msg);

					promocodes_array.add(promocodes_new);
					jo_cartInsert.put("promocodes_array", promocodes_array);
					jo_cartInsert.put("sub_total", Sub_total);
					jo_cartInsert.put("Discount", total_promo_Discount + promo_dis_value);
					jo_cartInsert.put("Tax", tax);
					jo_cartInsert.put("sub_total_before_tax", Sub_total - total_promo_Discount - promo_dis_value);
					jo_cartInsert.put("shipping", shipping);
					Grand_total = Sub_total + shipping + tax - total_promo_Discount - promo_dis_value;
					jo_cartInsert.put("Grand_total", Grand_total);

				}
				else {
					
					
					Str_msg = "Promocode  " + promocode + " alredy Applied";
					jo_cartInsert.put("status", "1");
					jo_cartInsert.put("message", Str_msg);
					
					jo_cartInsert.put("promocodes_array", promocodes_array);
					jo_cartInsert.put("sub_total", Sub_total);
					jo_cartInsert.put("Discount", total_promo_Discount + promo_dis_value);
					jo_cartInsert.put("Tax", tax);
					jo_cartInsert.put("sub_total_before_tax", Sub_total - total_promo_Discount - promo_dis_value);
					jo_cartInsert.put("shipping", shipping);
					Grand_total = Sub_total + shipping + tax - total_promo_Discount - promo_dis_value;
					jo_cartInsert.put("Grand_total", Grand_total);

					
					
					
				}

				stmt_insert.close();
			}

			
			
			
			conn.close();
		} catch (Exception e) {

			logger.log("Exception " + e);
			jo_cartInsert.put("status", "0");
			jo_cartInsert.put("message", e.getMessage());
		}

		finally {
			if (conn != null) {
				try {
					if (!conn.isClosed()) {

						conn.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
					logger.log("Exception" + e.toString());
					JSONObject jo_catch = new JSONObject();
					jo_catch.put("Exception", e.getMessage());
					return jo_catch;
				}
			}
		}
		return jo_cartInsert;
	}
}
