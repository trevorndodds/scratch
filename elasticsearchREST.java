
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONObject;

public class InsertIntoElasticREST {

	public static void main(String[] args) {
	String url = "" ;
  String indexName = "";
  String name = "";
  int socketTimeout = 10000;
  
    
    try {
        HttpResponse test = PostData(url, 9200, indexName, name, socketTimeout);
        System.out.println(test);
      } 
      catch (Exception e) {
      System.out.println("Error - " + e.getMessage());
		}
	}
  
public static HttpResponse PostData(String url, int port, String indexName, String name, int socketTimeout) throws Exception{
		Date today = Calendar.getInstance().getTime();
		
	  	DateFormat dfindexDate = new SimpleDateFormat("yyyy.MM");
	  	dfindexDate.setTimeZone(TimeZone.getTimeZone("UTC"));
    	String indexDate = dfindexDate.format(today);
    	
    	DateFormat dflogDate = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    	dflogDate.setTimeZone(TimeZone.getTimeZone("UTC"));
    	String logDateTime = dflogDate.format(today);
	
	    RequestConfig requestConfig = RequestConfig.custom()
	            .setConnectTimeout(socketTimeout)
	            .setSocketTimeout(socketTimeout)
	            .setConnectionRequestTimeout(socketTimeout)
	            .build();
	    
	    HttpClient httpClient = HttpClientBuilder.create()
				.disableAutomaticRetries()
				.setDefaultRequestConfig(requestConfig)
				.build();
		
		
		HttpPost postRequest = new HttpPost("http://"+ url +":" +port+"/"+indexName+"-"+indexDate + "/message");
		
		postRequest.addHeader("Accept", "application/json");
		postRequest.addHeader("Content-Type", "application/json");
		postRequest.setHeader("Connection", "close");
    
     JSONObject json = new JSONObject();
		 json.put("@timestamp", logDateTime);
		 json.put("name", name);
     StringEntity entity = new StringEntity(json.toString(), "UTF-8");
	   entity.setContentType("application/json");
	   postRequest.setEntity(entity);
     
	   try {
				HttpResponse response = httpClient.execute(postRequest);
				return response;
			} catch (ClientProtocolException e) {
				throw e;
			} catch (IOException e) {
				throw e;
			}
		    finally{
	        	postRequest.releaseConnection();
	        }	         
		}		
}