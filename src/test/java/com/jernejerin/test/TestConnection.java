package com.jernejerin.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class TestConnection {
	private final static String USER_AGENT = "Mozilla/5.0";
	public static void main (String[] args) throws URISyntaxException {
		try {
			CloseableHttpClient httpclient = HttpClients.createDefault();
			HttpGet httpGet = new HttpGet("https://instylewhatsrightnow.wordpress.com/feed/");
			httpGet.setHeader(HttpHeaders.USER_AGENT, "Mozilla/5.0 Firefox/26.0");
			//httpGet.addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
			CloseableHttpResponse response1 = httpclient.execute(httpGet);
			// The underlying HTTP connection is still held by the response object
			// to allow the response content to be streamed directly from the network socket.
			// In order to ensure correct deallocation of system resources
			// the user MUST either fully consume the response content  or abort request
			// execution by calling CloseableHttpResponse#close 	().
			

			try {
			    System.out.println(response1.getStatusLine());
			    HttpEntity entity1 = response1.getEntity();
			    // entity1.wr
			    // System.out.println(entity1.getContent().toString());
			    
			    // System.out.println(EntityUtils.toString(entity1));
			    BufferedReader in = new BufferedReader(
				        new InputStreamReader(entity1.getContent()));

		        String inputLine;
		        while ((inputLine = in.readLine()) != null)
		            System.out.println(inputLine);
		        in.close();
			    // do something useful with the response body
			    // and ensure it is fully consumed
			    EntityUtils.consume(entity1);
			} finally {
			    response1.close();
			}
			/*URL oracle = new URL("http://www.webdesignerdepot.com/2014/05/our-favorite-tweets-of-the-week-april-28-2014-may-4-2014/");
	        BufferedReader in = new BufferedReader(
	        new InputStreamReader(oracle.openStream()));

	        String inputLine;
	        while ((inputLine = in.readLine()) != null)
	            System.out.println(inputLine);
	        in.close();
	        */
			/*
			// if link exists we can fetch the whole entry (HTML page)
			URL obj = new URL("http://dalespeak.wordpress.com/2014/04/16/quarry-sidings-trackbed/");
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
	 
			// optional default is GET
			con.setRequestMethod("GET");
	 
			//add request header
			con.setRequestProperty("User-Agent", USER_AGENT);
	 
			if (con.getResponseCode() == 200) {
				BufferedReader in = new BufferedReader(
				        new InputStreamReader(con.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();
		 
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();
		 
				System.out.println(response.toString());
			}
			*/
		} catch (SocketException ex) {
			ex.printStackTrace();
		} catch (MalformedURLException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}
