package io.bittiger.ads;

import java.io.IOException;

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
	}
	
}
