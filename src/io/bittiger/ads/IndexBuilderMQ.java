package io.bittiger.ads;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class IndexBuilderMQ {
	
	
	public static void main(String[] args) throws Exception{
		 
		 IndexBuilder indexBuilder = new IndexBuilder("127.0.0.1",11211,"127.0.0.1:3306","searchads","root","dang"); ;
		 ConnectionFactory factory = new ConnectionFactory();
	     factory.setHost("127.0.0.1");
	     Connection connection = factory.newConnection();
	     Channel inChannel = connection.createChannel();
	     inChannel.basicQos(10); // Per consumer limit
         inChannel.queueDeclare("q_product", true, false, false, null);
   
        Consumer consumer = new DefaultConsumer(inChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("consumerTag:" + consumerTag);
                System.out.println("Received ," +"RoutingKey:"+envelope.getRoutingKey() + ",message: " + message);
                ObjectMapper mapper = new ObjectMapper();
                Ad ad = mapper.readValue(message, Ad.class);
                
                //build invert index
				indexBuilder.buildInvertIndex(ad);
				//build forward index
				indexBuilder.buildForwardIndex(ad);
            }
        };
        
        //consume ad
	    inChannel.basicConsume("q_product", true, consumer);
		
//		try (BufferedReader brAd = new BufferedReader(new FileReader(mAdsDataFilePath))) {
//			String line;
//			while ((line = brAd.readLine()) != null) {
//				JSONObject adJson = new JSONObject(line);
//				Ad ad = new Ad(); 
//				if(adJson.isNull("adId") || adJson.isNull("campaignId")) {
//					continue;
//				}
//				ad.adId = adJson.getLong("adId");
//				ad.campaignId = adJson.getLong("campaignId");
//				ad.brand = adJson.isNull("brand") ? "" : adJson.getString("brand");
//				ad.price = adJson.isNull("price") ? 100.0 : adJson.getDouble("price");
//				ad.thumbnail = adJson.isNull("thumbnail") ? "" : adJson.getString("thumbnail");
//				ad.title = adJson.isNull("title") ? "" : adJson.getString("title");
//				ad.detail_url = adJson.isNull("detail_url") ? "" : adJson.getString("detail_url");						
//				ad.bidPrice = adJson.isNull("bidPrice") ? 1.0 : adJson.getDouble("bidPrice");
//				ad.pClick = adJson.isNull("pClick") ? 0.0 : adJson.getDouble("pClick");
//				ad.category =  adJson.isNull("category") ? "" : adJson.getString("category");
//				ad.description = adJson.isNull("description") ? "" : adJson.getString("description");
//				ad.keyWords = new ArrayList<String>();
//				JSONArray keyWords = adJson.isNull("keyWords") ? null :  adJson.getJSONArray("keyWords");
//				for(int j = 0; j < keyWords.length();j++)
//				{
//					ad.keyWords.add(keyWords.getString(j));
//				}
//				
//				
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		
	}
	
}
