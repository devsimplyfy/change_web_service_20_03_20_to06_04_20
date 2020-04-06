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
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Shop_now_Cart_Disply implements RequestHandler<JSONObject, JSONObject> {
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
		if (!input.containsKey("device_id")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'device_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		JSONArray cart_Add_array = new JSONArray();
		JSONObject jsonObject_cartDisplay_result = new JSONObject();

		Object userid1 = input.get("userid");
		Object device_id1 = input.get("device_id");
		Object search1 = null;
		String device_id = null;
		String vendor_id = null;
		long userid;
		long product_id;

		String Str_msg = null;
		float Sub_total = 0, Tax = 0, shipping_carge = 0, Sub_total1 = 0;
		float total_promo_Discount = 0, Grand_Total = 0;
		String sql;
		JSONObject jo_cartInsert = new JSONObject();

		String first_name = null, last_name = null, address1 = null, address2 = null, address3 = null, city = "city",
				state = null, country = null, email_address = null;
		int pincode = 0, delevery_address_id = 0;

		String city_buyer = null, state_buyer = null, country_buyer = null, zone = null;
		int is_metro = 0, is_special_destination = 0, is_RoI_A = 0, is_RoI_B = 0;

		String city_seller = null, zone_seller = null, state_seller = null, country_seller = null;
		int is_metro_seller = 0, is_special_destination_seller = 0, is_RoI_A_seller = 0, is_RoI_B_seller = 0;

		float shipping_charge = 0, shipping = 0;
		int plan_id = 0, courier_id = 0;

		String courier_name = "BlueDart";
		String category_name = "Standard";
		Long phoneNumber;

		if (userid1 == null || userid1 == "") {
			userid = 0;
		} else {
			userid = Long.parseLong(userid1.toString());
		}

		if ((device_id1 == null || device_id1 == "") && userid == 0) {

			Str_msg = "Please Enter either UserId or Device_id";
			jo_cartInsert.put("status", "0");
			jo_cartInsert.put("message", Str_msg);
			return jo_cartInsert;

		} else {
			device_id = device_id1.toString();
		}

		Statement stmt = null;
		Connection con = null;

		Properties prop = new Properties();
		try {
			prop.load(getClass().getResourceAsStream("/application.properties"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			logger.log("Exception" + e1.toString());
		}

		DB_URL = prop.getProperty("url");
		USERNAME = prop.getProperty("username");
		PASSWORD = prop.getProperty("password");

		JSONArray jsonArray_product = new JSONArray();
		JSONObject jsonObject_product_Result = new JSONObject();
		JSONObject jsonObject_image = new JSONObject();

		try {
			con = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
			stmt = con.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			logger.log("Exception" + e1.toString());
		}

		JSONArray json_array_promocode = new JSONArray();

		// Get time from DB server
		try {

			if (userid != 0) {

				
				
				Statement stmt_customer = con.createStatement();
				ResultSet srs_customer = stmt_customer
						.executeQuery("SELECT id,status FROM customers where id='" + userid + "'");

				if (srs_customer.next() == false) {

					Str_msg = "No User Found !";
					jsonObject_cartDisplay_result.put("status", "0");
					jsonObject_cartDisplay_result.put("message", Str_msg);
					return jsonObject_cartDisplay_result;
/*
					errorPayload.put("errorType", "BadRequest");
					errorPayload.put("httpStatus", 400);
					errorPayload.put("requestId", context.getAwsRequestId());
					errorPayload.put("message", "No User Found !");
					throw new RuntimeException(errorPayload.toJSONString());
*/
					
				} else if (!srs_customer.getString("status").equalsIgnoreCase("1")) {
					Str_msg = "User not confirmed !";
					jsonObject_cartDisplay_result.put("status", "0");
					jsonObject_cartDisplay_result.put("message", Str_msg);
					return jsonObject_cartDisplay_result;

				}
				srs_customer.close();
				stmt_customer.close();
/*
				String sql_address_check = "SELECT id FROM address WHERE customerId = '" + userid + "' AND isPrimary=1";
				Statement stmt_customer_add_check = con.createStatement();
				ResultSet srs_customer_add_check = stmt_customer_add_check.executeQuery(sql_address_check);

				if (srs_customer_add_check.next()) {

					logger.log("Default Address Found");
				} else {

					Str_msg = "No default address found in user profile.";
					jsonObject_cartDisplay_result.put("status", "0");
					jsonObject_cartDisplay_result.put("message", Str_msg);
					//return jsonObject_cartDisplay_result;

				}
*/
				String sql_buyer = "SELECT pincodes.* FROM address INNER JOIN pincodes ON address.pincode=pincodes.pincode WHERE customerId='"
						+ userid + "' AND isPrimary=1";
				Statement stmt_customer_add = con.createStatement();
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

				sql = "SELECT products.*,table1.attribute_value,cart_items.UserId,cart_items.Quantity as cart_quantity,(cart_items.Quantity * products.sale_price) AS total,GROUP_CONCAT(product_offers.offer_name) as offer_name FROM products LEFT JOIN \n"
						+ " (SELECT pa.original_product_id,GROUP_CONCAT(att_group_name,':',av.att_value) AS attribute_value FROM product_attributes pa INNER JOIN attributes_value av ON av.id=pa.att_group_val_id INNER JOIN attributes a ON a.id=pa.att_group_id GROUP BY pa.original_product_id) AS table1 ON products.id=table1.original_product_id \n"
						+ " LEFT JOIN cart_items ON cart_items.ProductId=products.id  LEFT JOIN product_offers ON product_offers.product_id=products.id WHERE cart_items.UserId='"
						+ userid + "' GROUP BY products.id";

			} else {

				Statement stmt_device = con.createStatement();
				ResultSet srs_device = stmt_device.executeQuery(
						"SELECT * FROM cart_items where device_id='" + device_id + "' and UserId='" + userid + "'");

				if (srs_device.next() == false) {

					Str_msg = "Device Id not Valid";
					jsonObject_cartDisplay_result.put("status", "0");
					jsonObject_cartDisplay_result.put("message", Str_msg);
					return jsonObject_cartDisplay_result;
				}
				srs_device.close();
				stmt_device.close();

				sql = "SELECT products.*,table1.attribute_value,cart_items.UserId,cart_items.Quantity as cart_quantity,(cart_items.Quantity * products.sale_price) AS total,GROUP_CONCAT(product_offers.offer_name) as offer_name FROM products LEFT JOIN \n"
						+ " (SELECT pa.original_product_id,GROUP_CONCAT(att_group_name,':',av.att_value) AS attribute_value FROM product_attributes pa INNER JOIN attributes_value av ON av.id=pa.att_group_val_id INNER JOIN attributes a ON a.id=pa.att_group_id GROUP BY pa.original_product_id) AS table1 ON products.id=table1.original_product_id \n"
						+ " LEFT JOIN cart_items ON cart_items.ProductId=products.id  LEFT JOIN product_offers ON product_offers.product_id=products.id WHERE cart_items.device_id='"
						+ device_id + "' and cart_items.UserId='0' GROUP BY products.id";

			}

			// logger.log("\n SQL = \n " + sql);
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next() == false) {

				Str_msg = "cart_items are not present for this user/device_id ";
				jsonObject_cartDisplay_result.put("status", "0");
				jsonObject_cartDisplay_result.put("message", Str_msg);
				return jsonObject_cartDisplay_result;

			}

			JSONObject jo_CartItem_Result = new JSONObject();
			JSONArray json_array_CartItem = new JSONArray();

			do {

				JSONObject jo_cartItem = new JSONObject();
				JSONArray json_array_CartItem1 = new JSONArray();
				jo_cartItem.put("Product_id", rs.getString("id"));
				jo_cartItem.put("vendor_product_id", rs.getString("vendor_product_id"));
				jo_cartItem.put("quantity", rs.getString("cart_quantity"));
				jo_cartItem.put("sale_price", rs.getFloat("sale_price"));
				jo_cartItem.put("regular_price", rs.getFloat("regular_price"));
				jo_cartItem.put("name", rs.getString("name"));

				String offerArray[] = {};

				if (rs.getString("offer_name") == null) {

					jo_cartItem.put("offer", "");
				} else {

					offerArray = rs.getString("offer_name").split(",");
					jo_cartItem.put("offer", offerArray);
				}

				jo_cartItem.put("description", rs.getString("description"));
				jo_cartItem.put("total", rs.getFloat("total"));
				jo_cartItem.put("image", rs.getString("image"));

				String attribute_value = null;
				if (rs.getString("attribute_value") == null) {
					jo_cartItem.put("product_attribute", json_array_CartItem1);
					
				} else {
					//json_array_CartItem1.add(rs.getString("attribute_value"));
					//jo_cartItem.put("product_attribute", json_array_CartItem1);
					attribute_value=rs.getString("attribute_value");

				//jo_cartItem.put("product_attribute", json_array_CartItem1);
				
				
				String attr_key_val[];
				String attr_key[];
				JSONObject jo_cartItem_att = new JSONObject();
				if (attribute_value != null) {
					attr_key_val = attribute_value.split(",");
					
					for (int i = 0; i < attr_key_val.length; i++) {
						attr_key = attr_key_val[i].split(":");
						jo_cartItem_att.put(attr_key[0], attr_key[1]);
					}
					
					
					json_array_CartItem1.add(jo_cartItem_att);	
				
				}
				
				jo_cartItem.put("product_attribute", json_array_CartItem1);
				
				}
				Sub_total = Sub_total + rs.getFloat("total");
				Sub_total1 = Sub_total;

				json_array_CartItem.add(jo_cartItem);

			} while (rs.next());

			jo_CartItem_Result.put("Products", json_array_CartItem);
			jo_CartItem_Result.put("Sub_total", Sub_total);
			jo_CartItem_Result.put("Tax", "0");
			jo_CartItem_Result.put("Currency", "INR");
			jsonObject_cartDisplay_result.put("Cart_Items", jo_CartItem_Result);

		} catch (Exception e) {
			e.printStackTrace();
			logger.log("Caught exception: " + e.getMessage());
			JSONObject jo_catch = new JSONObject();
			jo_catch.put("Exception", e.getMessage());
			return jo_catch;
		}

		
		
		
		
		//code for promocode
		try {

			String sql_promocode;
			JSONObject jo_promocode = new JSONObject();

			Date date = new Date();
			long time = date.getTime();

			if (userid != 0) {

				sql_promocode = "SELECT promocodes.*,cart_promocode.* FROM cart_promocode INNER JOIN promocodes ON promocodes.id=cart_promocode.promocode_id WHERE user_id="
						+ userid + " and end_time>='" + time + "'and status=1";

				sql_promocode = "SELECT promocodes.*,cart_promocode.* FROM cart_promocode INNER JOIN promocodes ON promocodes.id=cart_promocode.promocode_id WHERE user_id='"
						+ userid + "' and status=1";

			} else {

				sql_promocode = "SELECT promocodes.*,cart_promocode.id AS cart_promo_id,cart_promocode.device_id,cart_promocode.user_id,cart_promocode.promocode_id FROM cart_promocode INNER JOIN promocodes ON promocodes.id=cart_promocode.promocode_id WHERE device_id='"
						+ device_id + "'  and status=1 and user_id=" + userid;

				// and end_time>='" + time + "'
			}

			// logger.log("\n sql_promocode \n " + sql_promocode);

			ResultSet rs_promocode = stmt.executeQuery(sql_promocode);
			ArrayList<Integer> arrlist_for_NA_products = new ArrayList<Integer>();

			while (rs_promocode.next()) {
				JSONObject jo_promocode1 = new JSONObject();

				String promocode = rs_promocode.getString("promo_value");
				long end_time = rs_promocode.getLong("end_time");

				if (end_time < time) {

					arrlist_for_NA_products.add(rs_promocode.getInt("id"));

					jo_promocode1.put("promocode", rs_promocode.getString("promocode"));
					jo_promocode1.put("description", rs_promocode.getString("description"));
					jo_promocode1.put("message",
							"Oops ! This promocode has expired. Kindly remove it before placing order.");

				} else {

					float prom = Float.parseFloat(promocode);
					String type = rs_promocode.getString("promo_value_type");

					jo_promocode1.put("promocode", rs_promocode.getString("promocode"));
					jo_promocode1.put("description", rs_promocode.getString("description"));

					float promovalue = 0;
					if (type.equals("Variable")) {

						Sub_total1 = Sub_total1 - (Sub_total1 * prom) / 100;

					} else {

						Sub_total1 = Sub_total1 - prom;
					}

				}

				json_array_promocode.add(jo_promocode1);

			}

			if (!arrlist_for_NA_products.isEmpty()) {

				JSONObject jo_catch = new JSONObject();
				jo_catch.put("Message", "Remove invalid " + arrlist_for_NA_products + " promocode 1st");
				jo_catch.put("status", "0");

			}

			total_promo_Discount = Sub_total - Sub_total1;

			// Logic for shipping Charge

			if (city.equalsIgnoreCase("city")==false) {

				String sql_srs_vendor = null;
				if (userid != 0) {
					sql_srs_vendor = "SELECT DISTINCT (VendorId) FROM cart_items WHERE UserId='" + userid + "'";

					 logger.log("sql_srs_vendor\n" + sql_srs_vendor);

					Statement stmt_vendor_shipping = con.createStatement();
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

						 logger.log("\n pincode_Sql_seller \n" + pincode_Sql_seller);

						Statement stmt_pincode_seller = con.createStatement();
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

							 logger.log("\n we are in else \n" + city_type);

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

						 logger.log("\n city_type \n" + city_type);

						String sql_shipping = "SELECT shipping_categories.*,couriers.courier_name FROM couriers  INNER JOIN shipping_categories ON shipping_categories.courier_id=couriers.id WHERE courier_name='"
								+ courier_name + "' AND category_name='" + category_name
								+ "' AND weight='0-500 gms' AND vendor_id='" + vendorId_charg + "'";

						 logger.log("\n sql_shipping \n" + sql_shipping);

						Statement stmt_shipping = con.createStatement();
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

			Grand_Total = (Sub_total - total_promo_Discount) + Tax + shipping;

			if (Grand_Total >= 0) {

				jsonObject_cartDisplay_result.put("Grand_Total", Grand_Total);
				jsonObject_cartDisplay_result.put("Shipping", shipping);
				jsonObject_cartDisplay_result.put("sub_total_before_tax", Sub_total-total_promo_Discount);
				jsonObject_cartDisplay_result.put("total_promo_Discount", total_promo_Discount);
				

			} else {

				jsonObject_cartDisplay_result.put("Grand_Total",
						"Grand Total cannot be calculated as promocode discount is higher than Sub Total");
				return jsonObject_cartDisplay_result;

			}

			con.close();
		}catch (RuntimeException ex) {
			
		}
		catch (Exception e) {
			e.printStackTrace();
			logger.log("Caught exception: " + e.getMessage());
			JSONObject jo_catch = new JSONObject();
			jo_catch.put("Exception", e.getMessage());
			return jo_catch;

		} finally {
			if (con != null) {
				try {
					if (!con.isClosed()) {

						con.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
					JSONObject jo_catch = new JSONObject();
					jo_catch.put("Exception", e.getMessage());
					return jo_catch;
				}
			}
		}
		jsonObject_cartDisplay_result.put("promocode", json_array_promocode);
		return jsonObject_cartDisplay_result;

	}
}
