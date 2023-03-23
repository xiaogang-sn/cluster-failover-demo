package io.streamnative.data;

import java.sql.Timestamp;
import java.util.Date;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

@SuppressWarnings("unused")
public class DataGenerator implements Runnable {

    private final Producer<String> producer;

    public DataGenerator(Producer<String> producer) {
        this.producer = producer;
    }

    @Override
    public void run() {
        int counter = 0;
        while (true) {
            try {
            	String msgString = "Message-" + counter++; 
            	Date date = new Date();
            	System.out.println(new Timestamp(date.getTime()) + " Producing message : " + msgString);
                producer.newMessage().value(msgString).send();
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
