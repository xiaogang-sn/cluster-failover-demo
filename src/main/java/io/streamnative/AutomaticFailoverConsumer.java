package io.streamnative;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.impl.AutoClusterFailover;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

import io.streamnative.data.DataReceiver;

import java.net.*;

public class AutomaticFailoverConsumer extends FailoverDemoBase {
	
    public PulsarClient getClient() throws PulsarClientException, MalformedURLException {
        if (client == null) {
        	
        	URL issuerUrl = new URL("https://auth.sncloud-stg.dev/");
        	URL credentialsUrl = new URL("file:///Users/xiaogang/SN_Work/Workspaces/o-60j05-admin.json");
        	String audience = "urn:sn:pulsar:o-60j05:test1";
        	
        	URL issuerUrlSecondary = new URL("https://auth.sncloud-stg.dev/");
        	URL credentialsUrlSecondary = new URL("file:///Users/xiaogang/SN_Work/Workspaces/sndev-admin.json");
        	String audienceSecondary = "urn:sn:pulsar:sndev:xgdemo";
        	
        	Map<String, Authentication> secondaryAuthentications = new HashMap<>();
        	secondaryAuthentications.put(STANDBY_BROKER_URL, AuthenticationFactoryOAuth2.clientCredentials(issuerUrlSecondary, credentialsUrlSecondary, audienceSecondary));
        	
            ServiceUrlProvider failover = AutoClusterFailover.builder()
                    .primary(ACTIVE_BROKER_URL)
                    .secondary(Collections.singletonList(STANDBY_BROKER_URL))
                    .failoverDelay(20, TimeUnit.SECONDS)
                    .switchBackDelay(20, TimeUnit.SECONDS)
                    .checkInterval(1000, TimeUnit.MILLISECONDS)
                    .secondaryAuthentication(secondaryAuthentications)
                    .build();

            client = PulsarClient.builder()
                    .serviceUrlProvider(failover)
                    .authentication(
                            AuthenticationFactoryOAuth2.clientCredentials(issuerUrl, credentialsUrl, audience))
                    .build();
        }
        return client;
    }

    public static void main(String[] args) throws PulsarClientException, MalformedURLException {
        AutomaticFailoverConsumer demo = new AutomaticFailoverConsumer();
        DataReceiver receiver = new DataReceiver(demo.getConsumer(TOPIC_NAME, SUBSCRIPTION_NAME));
        receiver.run();
    }
}
