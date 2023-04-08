package io.streamnative;

import java.net.MalformedURLException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.*;

public abstract class FailoverDemoBase {

    protected static final String TOPIC_NAME = "persistent://geo/ns/failover";
    protected static final String ACTIVE_BROKER_URL = "pulsar+ssl://test1-71796523-39de-44ba-8940-e452d69a54cd.use2-lion.aws.sn3.dev:6651";
    protected static final String STANDBY_BROKER_URL = "pulsar+ssl://xgdemo-4df16e69-c8fa-487d-9adf-71c22fe1dad6.use4-dog.g.sn3.dev:6651";
    protected static final String SUBSCRIPTION_NAME = "my_subscription";

    protected PulsarClient client;
    protected Producer<String> producer;
    protected Consumer<String> consumer;

    public abstract PulsarClient getClient() throws PulsarClientException, MalformedURLException;

    public Producer<String> getProducer(String topicName) throws PulsarClientException, MalformedURLException {
        if (producer == null) {
            producer = getClient().newProducer(Schema.STRING)
                    .topic(topicName)
                    .sendTimeout(0, TimeUnit.SECONDS)
                    .create();
        }
        return producer;
    }
    
    public Consumer<String> getConsumer(String topicName, String subscriptionName) throws PulsarClientException, MalformedURLException {
        if (consumer == null) {
            consumer = getClient().newConsumer(Schema.STRING)
                    .topic(topicName)
                    .subscriptionName(subscriptionName)
                    .replicateSubscriptionState(true)
                    .subscribe();
        }
        return consumer;
    }
}
