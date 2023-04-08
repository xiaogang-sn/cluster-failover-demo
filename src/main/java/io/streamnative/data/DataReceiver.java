package io.streamnative.data;

import java.sql.Timestamp;
import java.util.Date;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

@SuppressWarnings("unused")

public class DataReceiver implements Runnable {

    private final Consumer<String> consumer;

    public DataReceiver(Consumer<String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
    	while (true) {
  		  // Wait for a message
			@SuppressWarnings("rawtypes")
			Message msg = null;
			try {
				msg = consumer.receive();
			} catch (PulsarClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		  try {
    		      // Do something with the message
    			  Date date = new Date();
    		      System.out.println(new Timestamp(date.getTime()) + " Message received: " + new String(msg.getData()));
    		      // Acknowledge the message so that it can be deleted by the message broker
    		      consumer.acknowledge(msg);
    		  } catch (Exception e) {
    		      // Message failed to process, re-deliver later
    		      consumer.negativeAcknowledge(msg);
    		  }
    		}
    }

}
