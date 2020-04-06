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

public class Shop_now_product_list implements RequestHandler<JSONObject, JSONObject> {
	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;
	public int flag_for_connection = 0;

	@SuppressWarnings("unchecked")
	public JSONObject handleRequest(JSONObject input, Context context) {

		JSONObject errorPayload = new JSONObject();
		LambdaLogger logger = context.getLogger();

		if (!input.containsKey("id")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("customer_id")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'customer_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("order")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'order' is missing");
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
		if (!input.containsKey("min_price")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'min_price' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("max_price")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'max_price' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}

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
		Connection conn = null;
		
		String sql4 = "";
		JSONArray ja_product_list = new JSONArray();
		JSONObject jo_product_list_result = new JSONObject();

		Statement stmt = null;
		Statement stmt_for_total = null;
		ResultSet resultSet;
		ResultSet resultSet_for_total;
		String attribute_str="";

		final int id;
		if (input.get("id") != null && input.get("id") != "") {
			id = Integer.parseInt(input.get("id").toString());
		} else {
			id = 0;
		}

		int page_number;
		if (input.get("page_number") != null && input.get("page_number") != "") {
			page_number = Integer.parseInt(input.get("page_number").toString());
		} else {

			page_number = 0;
		}

		int customerId = 0;
		if (input.get("customer_id") != null && input.get("customer_id") != "") {
			customerId = Integer.parseInt(input.get("customer_id").toString());
		} else {
			customerId = 0;

		}

		float min_price = 0;
		if (input.get("min_price") != null && input.get("min_price") != "") {
			min_price = Float.parseFloat(input.get("min_price").toString());
		} else {
			min_price = 0;
		}

		float max_price = 0;
		if (input.get("max_price") != null && input.get("max_price") != "") {
			max_price = Float.parseFloat(input.get("max_price").toString());
			
			if(max_price<0) {
				
				jo_product_list_result.put("status", "0");
				jo_product_list_result.put("message", "No products found which have max_price < 0! ");
				return jo_product_list_result;
					
			}
		} else {
			max_price = -1;
		}

		String search;
		if (input.get("search") != null && input.get("search") != "") {
			search = input.get("search").toString();
		} else {

			search = "";
		}
		if (search.contains("'")) {
			search = search.replaceAll("'", "''");
		}		
		
		String orderBy;
		if (input.get("order") != null) {
			orderBy = input.get("order").toString();
		} else {
			orderBy = null;
		}

		
		
		String brand_id1="";
		if (input.get("brand_id") != null && input.get("brand_id") != "") {

			brand_id1 = input.get("brand_id").toString();

		} 

		String attribute="";
		if (input.get("attribute") != null && input.get("attribute") != "") {

			attribute = input.get("attribute").toString();

		} 
		
		

		// This Logic for Product Per Page
		int page_size = 50;
		if (page_number == 0) {
			page_number = 1;

		}
		int page_offset = (page_number - 1) * page_size;

		if (search == null) {
			search = "";
		}

		if (min_price == 0) {
			min_price = 0;

		}
	/*	if (min_price == 0 && max_price==-1) {

			jo_product_list_result.put("status", "0");
			jo_product_list_result.put("message", "No products found ! ");
			return jo_product_list_result;

		}
*/
						
				// =====================================================================================================================================
		
		String max_price1="",min_price1="",category_id="";			
		if(id>0){
			
			category_id= " AND category_id=" + id;
			
		}
				
		if(max_price>0) {  max_price1="And sale_price <= "+max_price;	}
		
		if(min_price>-1) {  min_price1=" And sale_price >= "+min_price;	 }
	
		if(!brand_id1.equals("")) { brand_id1="  And brand_id in ( "+brand_id1+") "; }
			
		String attribute_str1 = null;
		int attribute_flag=0;
		String total_products_sql = "select COUNT(*) AS total_products from (SELECT  id FROM products WHERE NAME LIKE '%"+ search + "%' " + max_price1 + min_price1+brand_id1+category_id+" and do_not_display='0' ) as table_without_att "+ attribute_str;
		if(!attribute.equals("")) {	
			
			attribute_flag=1;
			attribute_str=" INNER JOIN(SELECT product_attributes.original_product_id FROM product_attributes INNER JOIN `attributes_value` ON product_attributes.att_group_val_id=attributes_value.id where attributes_value.id in( "+attribute+")  ) AS table_att ON table_without_att.id=table_att.original_product_id ";
			
			attribute_str1=" INNER JOIN(SELECT product_attributes.original_product_id FROM product_attributes INNER JOIN `attributes_value` ON product_attributes.att_group_val_id=attributes_value.id where attributes_value.id in( "+attribute+")  ) AS table_att ON table_without_att.id=table_att.original_product_id limit " + page_offset+ "," + page_size + "";
			
			total_products_sql="SELECT COUNT(DISTINCT(original_product_id)) AS total_products FROM (SELECT  id FROM products WHERE NAME LIKE '%%'  AND sale_price >= 0.0 AND do_not_display='0' ) AS table_without_att  INNER JOIN(SELECT product_attributes.original_product_id,attributes_value.att_value ,attributes_value.id FROM product_attributes INNER JOIN `attributes_value` ON product_attributes.att_group_val_id=attributes_value.id WHERE attributes_value.id IN( 2003321559891966749) ) AS table_att ON table_without_att.id=table_att.original_product_id";
		}
				
				
	
		
		System.out.println("\n total_products_sql"+ total_products_sql );

	

	int flagChange = 0;
	String order = "ASC";

	if (orderBy == null) {
		order = " id ASC";
		flagChange = 0;
	} else if (orderBy.equalsIgnoreCase("priceup")) {
		order = "sale_price ASC";
		flagChange = 1;
	} else if (orderBy.equalsIgnoreCase("pricedown")) {
		order = "sale_price DESC";
		flagChange = 1;
	} else if (orderBy.equalsIgnoreCase("AtoZ")) {
		order = " name ASC";
		flagChange = 2;

	} else if (orderBy.equalsIgnoreCase("ZtoA")) {
		order = " name DESC";
		flagChange = 2;

	} else {
		order = "id ASC";
		flagChange = 0;

	}
	
	
	
	

	// =====================================================================================================================================

					if (customerId == 0) {
								System.out.println("flagChange=0,id=0 max = 0 customer_id=0 brand_id=0,attribute=0");

								if(attribute_flag==0) {
								
								 sql4 = "select * from (SELECT table1.*,(SELECT SUM(c1) AS c FROM (SELECT category_id,COUNT(category_id) AS c1 FROM products WHERE do_not_display='0'  GROUP BY category_id) AS table_prod) AS total,0 AS wishlist FROM\n"
										+ "(SELECT p1.*,GROUP_CONCAT(im.image) AS image1 FROM products AS p1\n"
										+ "LEFT JOIN product_image AS im ON p1.id=im.product_id WHERE  NAME LIKE '%"
										+ search + "%'  AND do_not_display='0' "+brand_id1+ category_id+min_price1+max_price1+" GROUP BY id order by " + order + " limit " + page_offset+ "," + page_size + ")AS table1\n"
										+ "LEFT JOIN (SELECT category_id,COUNT(category_id) AS c FROM products WHERE do_not_display='0' GROUP BY category_id) AS t1 ON t1.category_id=table1.category_id )  AS table_without_att "+ attribute_str;
								}
								else {
									
									 sql4 = "select DISTINCT(id),wishlist,NAME,description,regular_price,sale_price,stock,image,image1 from (SELECT table1.*,(SELECT SUM(c1) AS c FROM (SELECT category_id,COUNT(category_id) AS c1 FROM products WHERE do_not_display='0'  GROUP BY category_id) AS table_prod) AS total,0 AS wishlist FROM\n"
												+ "(SELECT p1.*,GROUP_CONCAT(im.image) AS image1 FROM products AS p1\n"
												+ "LEFT JOIN product_image AS im ON p1.id=im.product_id WHERE  NAME LIKE '%"
												+ search + "%'  AND do_not_display='0' "+brand_id1+ category_id+min_price1+max_price1+" GROUP BY id order by " + order + ")AS table1\n"
												+ "LEFT JOIN (SELECT category_id,COUNT(category_id) AS c FROM products WHERE do_not_display='0' GROUP BY category_id) AS t1 ON t1.category_id=table1.category_id )  AS table_without_att "+ attribute_str1;
										
									
									
								}
							
								 System.out.println(sql4);

							} else {
								System.out.println("flagChange=0,id=0 max = 0 customer_id=1");
								
								if(attribute_flag==0) {
							
								sql4 = "select DISTINCT(id),wishlist,NAME,description,regular_price,sale_price,stock,image,image1 from (SELECT table1.*,(SELECT SUM(c1) AS c FROM (SELECT category_id,COUNT(category_id) AS c1 FROM products WHERE do_not_display='0' GROUP BY category_id) AS table_prod) AS total,(CASE WHEN wish_list1.product_id IS NOT NULL THEN 1 ELSE 0 END) AS wishlist FROM\n"
										+ "(SELECT p1.*,GROUP_CONCAT(im.image) AS image1 FROM products AS p1\n"
										+ "LEFT JOIN product_image AS im ON p1.id=im.product_id WHERE  NAME LIKE '%"
										+ search + "%' AND do_not_display='0' "+brand_id1+ category_id+min_price1+max_price1+" GROUP BY id order by " + order + " limit " + page_offset+ "," + page_size + ")AS table1\n"
										+ "LEFT JOIN (SELECT product_id FROM wish_list WHERE customer_id="
										+ customerId + ") AS wish_list1 ON wish_list1.product_id=table1.id\n"
										+ "LEFT JOIN (SELECT category_id,COUNT(category_id) AS c FROM products WHERE do_not_display='0' GROUP BY category_id) AS t1 ON t1.category_id=table1.category_id )  AS table_without_att "+ attribute_str;
								
								 System.out.println(sql4);
								}else {
									
									sql4 = "select DISTINCT(id),wishlist,NAME,description,regular_price,sale_price,stock,image,image1 from (SELECT table1.*,(SELECT SUM(c1) AS c FROM (SELECT category_id,COUNT(category_id) AS c1 FROM products WHERE do_not_display='0' GROUP BY category_id) AS table_prod) AS total,(CASE WHEN wish_list1.product_id IS NOT NULL THEN 1 ELSE 0 END) AS wishlist FROM\n"
											+ "(SELECT p1.*,GROUP_CONCAT(im.image) AS image1 FROM products AS p1\n"
											+ "LEFT JOIN product_image AS im ON p1.id=im.product_id WHERE  NAME LIKE '%"
											+ search + "%' AND do_not_display='0' "+brand_id1+ category_id+min_price1+max_price1+" GROUP BY id order by " + order + ")AS table1\n"
											+ "LEFT JOIN (SELECT product_id FROM wish_list WHERE customer_id="
											+ customerId + ") AS wish_list1 ON wish_list1.product_id=table1.id\n"
											+ "LEFT JOIN (SELECT category_id,COUNT(category_id) AS c FROM products WHERE do_not_display='0' GROUP BY category_id) AS t1 ON t1.category_id=table1.category_id )  AS table_without_att "+ attribute_str1;
									
									
									 System.out.println(sql4);

									
									
								}
														
							}
					
				 

			 // end brand loop



		
		// ========================================================================================================================================

		try {
			conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
			flag_for_connection = 1;
			
			
			
			if(customerId!=0) {
			
			Statement stmt_customer = conn.createStatement();
			ResultSet srs_customer = stmt_customer
					.executeQuery("SELECT id, status FROM customers where id='" + customerId + "'");
			String Str_msg=null;
			if (srs_customer.next() == false) {

				Str_msg = "No user found!";
				jo_product_list_result.put("status", "0");
				jo_product_list_result.put("message", Str_msg);
				return jo_product_list_result;
			}

			else if (!srs_customer.getString("status").equalsIgnoreCase("1")) {
				Str_msg = "User not confirmed !";
				jo_product_list_result.put("status", "0");
				jo_product_list_result.put("message", Str_msg);
				return jo_product_list_result;
			}

			srs_customer.close();
			stmt_customer.close();

			}	
			
			
			
			
			stmt = conn.createStatement();
			resultSet = stmt.executeQuery(sql4); 
			int flag_for_total = 0;

			while (resultSet.next()) {
				flag_for_total = 1;
				JSONObject jo_product_list = new JSONObject();

				jo_product_list.put("id", resultSet.getString("id"));
				jo_product_list.put("wishlist", resultSet.getString("wishlist"));
				jo_product_list.put("name", resultSet.getString("name"));
				jo_product_list.put("description", resultSet.getString("description"));
				jo_product_list.put("regular_price", resultSet.getFloat("regular_price"));
				jo_product_list.put("sale_price", resultSet.getFloat("sale_price"));
				jo_product_list.put("stock", resultSet.getString("stock"));
				jo_product_list.put("image", resultSet.getString("image"));
				jo_product_list.put("Currency", "INR");
				// jo_product_list.put("Currency", "INR");

				String image_url = resultSet.getString("image1");

				JSONObject joimage = new JSONObject();
				JSONArray jaimage = new JSONArray();

				String[] image_url1 = null;
				if (image_url == null) {

					joimage.put("image", "NA");
					jaimage.add(joimage);
				} else {
					image_url1 = image_url.split(",");

					for (int k = 0; k < image_url1.length; k++) {
						JSONObject joimage1 = new JSONObject();
						joimage1.put("image", image_url1[k]);
						jaimage.add(joimage1);
					}

				}
				jo_product_list.put("image_extra", jaimage);
				ja_product_list.add(jo_product_list);

			}

			resultSet.close();
			stmt.close();

			if (flag_for_total == 1) {
				
				
				stmt_for_total = conn.createStatement();
				
				
				resultSet_for_total = stmt_for_total.executeQuery(total_products_sql);
				while (resultSet_for_total.next()) {
					jo_product_list_result.put("total", resultSet_for_total.getInt("total_products"));
				}
				stmt_for_total.close();
				resultSet_for_total.close();
			} else {
				jo_product_list_result.put("total", 0);
			}

			conn.close();
		} catch (Exception e) {

			e.printStackTrace();
			logger.log("Caught exception: " + e);
			JSONObject jo_catch = new JSONObject();
			jo_catch.put("Exception", e.getMessage());

			return jo_catch;

		}

		jo_product_list_result.put("products", ja_product_list);
		return jo_product_list_result;

	}

}
