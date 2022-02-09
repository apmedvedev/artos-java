package com.artos.api.shards.client.conf;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.spi.core.conf.ClientStoreConfiguration;
import com.artos.spi.core.conf.MessageSenderFactoryConfiguration;
import com.artos.spi.shards.conf.HashFunctionConfiguration;

import java.util.List;

public class StaticDataSpaceClientChannelConfigurationBuilder<K> {
    private String endpoint;
    private HashFunctionConfiguration<K> hashFunction = new SimpleHashFunctionConfiguration<K>();
    private List<GroupConfiguration> groups;
    private MessageSenderFactoryConfiguration messageSender;
    private ClientStoreConfiguration clientStore;

    public String getEndpoint() {
        return endpoint;
    }

    public StaticDataSpaceClientChannelConfigurationBuilder setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public HashFunctionConfiguration<K> getHashFunction() {
        return hashFunction;
    }

    public StaticDataSpaceClientChannelConfigurationBuilder setHashFunction(HashFunctionConfiguration<K> hashFunction) {
        this.hashFunction = hashFunction;
        return this;
    }

    public List<GroupConfiguration> getGroups() {
        return groups;
    }

    public StaticDataSpaceClientChannelConfigurationBuilder setGroups(List<GroupConfiguration> groups) {
        this.groups = groups;
        return this;
    }

    public MessageSenderFactoryConfiguration getMessageSender() {
        return messageSender;
    }

    public StaticDataSpaceClientChannelConfigurationBuilder setMessageSender(MessageSenderFactoryConfiguration messageSender) {
        this.messageSender = messageSender;
        return this;
    }

    public ClientStoreConfiguration getClientStore() {
        return clientStore;
    }

    public StaticDataSpaceClientChannelConfigurationBuilder setClientStore(ClientStoreConfiguration clientStore) {
        this.clientStore = clientStore;
        return this;
    }

    public StaticDataSpaceClientChannelConfiguration toConfiguration() {
        return new StaticDataSpaceClientChannelConfiguration(getEndpoint(), groups, getHashFunction(), messageSender, clientStore);
    }
}
