package com.shopNow.Lambda;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Shop_now_Banner_Detail implements RequestHandler<JSONObject, JSONObject> {

	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;

	@SuppressWarnings("unchecked")
	public JSONObject handleRequest(JSONObject j, Context context) {
		JSONObject errorPayload = new JSONObject();

		if (!j.containsKey("banner_id")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'banner_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		Object id;
		LambdaLogger logger = context.getLogger();
		String customer_id = j.get("customer_id").toString();

		logger.log("\n customer_id \n " + customer_id);
		int wishlist_flag = 0;
		if (customer_id.equalsIgnoreCase("null")||customer_id.equalsIgnoreCase("")) {
			wishlist_flag = 1;

		}

		if (j.get("banner_id") != null && j.get("banner_id") != "") {
			id = j.get("banner_id");
		} else {
			id = 0;
		}
		

		// logger.log("\n Invoked products Start " + id);
		ResultSet resultSet = null, resultSet_product_list, resultSet_products = null;
		String banner_products_id = null, banner_product = null, banner_deal = null;
		int total = 0;

		Statement stmt = null, stmt1 = null, stmt_SQL_products = null;
		Connection con = null;

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

		JSONObject jsonObject_product_Result = new JSONObject();

		try {
			con = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
			stmt = con.createStatement();
		} catch (SQLException e1) {

			e1.printStackTrace();
			logger.log("Caught exception: " + e1);
			JSONObject jo_catch = new JSONObject();
			jo_catch.put("Exception", e1.getMessage());
			return jo_catch;
		}

		try {
			int flag=0;
			String SQL_Product_ids = "select * from banners where id='" + id + "'";
			//logger.log("\n SQL_Product_ids " + SQL_Product_ids);
			resultSet = stmt.executeQuery(SQL_Product_ids);
			if (resultSet.next()) {

				banner_products_id = resultSet.getString("product_ids");
				banner_product = resultSet.getString("products");
				
				logger.log("\n products" + banner_product);
				banner_deal = resultSet.getString("deal");
				//logger.log("\n banner_deal" + banner_deal);

				if ((banner_products_id != null || banner_products_id != "") && (banner_product == null || banner_product == "") && (banner_deal == null||banner_deal == "")) {
				
					flag=1;
				
					
				
				} else if (banner_products_id == null && banner_product != null && banner_deal == null) {

					
					logger.log("\n banner_cat "+banner_product);
					String SQL_products = null;
					
					String[] similar_product_array =banner_product.split(",");
					String banner_cat_value=  banner_product.substring(banner_product.indexOf(':')+1);
					
					
					logger.log("\n banner_cat "+banner_cat_value);
					logger.log("\n similar_product_array.length "+similar_product_array.length);
				
					String str_banner_cat_value=null;
					if (similar_product_array.length>1) {
						int case1=0,case2=0,case3=0;
						
					  banner_cat_value=  banner_product.substring(banner_product.indexOf(':')+1);
						
						String banner_max_value = null,banner_min_value=null;
						for(int i=0;i<similar_product_array.length;i++) {
						
							if(similar_product_array[i].contains("category")) {
								
								str_banner_cat_value=similar_product_array[i].substring(banner_product.indexOf(':')+1);
								case1=1;
						
							}
							else if(similar_product_array[i].contains("maxprice")){
								
								banner_max_value=similar_product_array[i].substring(banner_product.indexOf(':')+1);
								case2=2;
							}
							else if(similar_product_array[i].contains("minprice")) {
								
								banner_min_value=similar_product_array[i].substring(banner_product.indexOf(':')+1);
								case3=3;	
							}
						
						}
						
						if(case1==1 && case2==2 && case3==3) {
						
						SQL_products = "SELECT GROUP_CONCAT(id) as id FROM products WHERE category_id IN(SELECT id FROM categories WHERE parent_id=(SELECT id FROM categories WHERE NAME='"+str_banner_cat_value+"')) and  sale_price<="+banner_max_value+" and sale_price>="+banner_min_value ;
						}
						else if(case1==1 && case2==2 && case3==0) {
							
							SQL_products = "SELECT GROUP_CONCAT(id) as id FROM products WHERE category_id IN(SELECT id FROM categories WHERE parent_id=(SELECT id FROM categories WHERE NAME='"+str_banner_cat_value+"')) and  sale_price<="+banner_max_value;
							}
						else if(case1==0 && case2==2 && case3==3){
							
							SQL_products = "SELECT GROUP_CONCAT(id) as id FROM products WHERE   sale_price<="+banner_max_value+" and sale_price>="+banner_min_value ;
							
						}
						
						
						logger.log("\n "+SQL_products + "SQL_products ");
					}		
					else if ((banner_product.contains("category")) && (banner_product.contains("maxprice"))) {
								

						SQL_products = "SELECT GROUP_CONCAT(id) as id FROM products WHERE category_id IN(SELECT id FROM categories WHERE parent_id=(SELECT id FROM categories WHERE NAME='"+banner_cat_value+"'))";

					} else if (banner_product.contains("maxprice")) {

						SQL_products = "SELECT GROUP_CONCAT(id) as id FROM products WHERE sale_price<="+banner_cat_value;

					} else if (banner_product.contains("minprice")) {

						SQL_products = "SELECT GROUP_CONCAT(id) as id FROM products WHERE sale_price>="+banner_cat_value;
					} else {

						jsonObject_product_Result.put("message", "Banner category not valid");
						jsonObject_product_Result.put("status", "0");
						return jsonObject_product_Result;

					}

					//logger.log("\n SQL_products " +SQL_products);
					stmt_SQL_products = con.createStatement();
					resultSet_products = stmt_SQL_products.executeQuery(SQL_products);

					if (resultSet_products.next()) {

						banner_products_id = resultSet_products.getString("id");
						//logger.log("\n products" + banner_products_id);

					} else {

						jsonObject_product_Result.put("message", "Banner id not presant product");
						jsonObject_product_Result.put("status", "0");
						return jsonObject_product_Result;

					}

					resultSet_products.close();
					stmt_SQL_products.close();

				} else if (banner_products_id == null && banner_product == null && banner_deal != null) {

				
					
					
					
					
					
				
				
				} else {

					jsonObject_product_Result.put("message", "Banner not valid");
					jsonObject_product_Result.put("status", "0");
					return jsonObject_product_Result;

				}

			} else {

				jsonObject_product_Result.put("message", "Banner id not valid");
				jsonObject_product_Result.put("status", "0");
				return jsonObject_product_Result;

			}
			resultSet.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block 11477759
			e.printStackTrace();
			logger.log("Caught exception: " + e);
			JSONObject jo_catch = new JSONObject();
			jo_catch.put("Exception", e.getMessage());
			return jo_catch;
		}

		// logger.log("\n Invoked products offer");

		try {
			String SQl_product_list = "SELECT p1.*,GROUP_CONCAT(im.image) AS image1 from products as p1 LEFT JOIN product_image AS im ON p1.id=im.product_id WHERE p1.id in ("
					+ banner_products_id + ") GROUP BY p1.id";
			
			
			SQl_product_list="SELECT p2.*,(CASE WHEN wish_list1.product_id IS NOT NULL THEN 1 ELSE 0 END) AS wishlist FROM \r\n" + 
					"(SELECT p1.*,GROUP_CONCAT(im.image) AS image1 FROM products AS p1 LEFT JOIN product_image AS im ON p1.id=im.product_id WHERE p1.id IN ("+ banner_products_id + ") GROUP BY p1.id) AS p2 LEFT JOIN\r\n" + 
					"(SELECT product_id FROM wish_list WHERE customer_id='"+customer_id+"') AS wish_list1 ON wish_list1.product_id=p2.id;\r\n" + 
					"";
			//logger.log("\n Invoked products offer_SQL \n" + SQl_product_list);
		
			stmt1 = con.createStatement();
			resultSet_product_list = stmt1.executeQuery(SQl_product_list);
			JSONArray ja_product_list = new JSONArray();

			while (resultSet_product_list.next()) {
				total = total + 1;
				JSONObject jo_product_list = new JSONObject();

				jo_product_list.put("id", resultSet_product_list.getInt("id"));
				 jo_product_list.put("wishlist", resultSet_product_list.getInt("wishlist"));
				jo_product_list.put("name", resultSet_product_list.getString("name"));
				jo_product_list.put("description", resultSet_product_list.getString("description"));
				jo_product_list.put("regular_price", resultSet_product_list.getFloat("regular_price"));
				jo_product_list.put("sale_price", resultSet_product_list.getFloat("sale_price"));
				jo_product_list.put("stock", resultSet_product_list.getString("stock"));
				jo_product_list.put("image", resultSet_product_list.getString("image"));
				

				String image_url = resultSet_product_list.getString("image1");

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
			jsonObject_product_Result.put("products", ja_product_list);
			resultSet_product_list.close();
			stmt1.close();

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			logger.log("Caught exception: " + e1);
			JSONObject jo_catch = new JSONObject();
			jo_catch.put("Exception", e1.getMessage());
			return jo_catch;
		}

		// logger.log("\n Invoked products attribute");
		jsonObject_product_Result.put("Currency", "INR");
		jsonObject_product_Result.put("total", total);

		return jsonObject_product_Result;
	}
}