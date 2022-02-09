package com.artos.api.core.client.conf;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.spi.core.conf.ClientStoreConfiguration;
import com.artos.spi.core.conf.MessageSenderFactoryConfiguration;

public class ClientChannelConfigurationBuilder {
    private String endpoint;
    private GroupConfiguration group;
    private MessageSenderFactoryConfiguration messageSender;
    private ClientStoreConfiguration clientStore;

    public String getEndpoint() {
        return endpoint;
    }

    public ClientChannelConfigurationBuilder setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public GroupConfiguration getGroup() {
        return group;
    }

    public ClientChannelConfigurationBuilder setGroup(GroupConfiguration group) {
        this.group = group;
        return this;
    }

    public MessageSenderFactoryConfiguration getMessageSender() {
        return messageSender;
    }

    public ClientChannelConfigurationBuilder setMessageSender(MessageSenderFactoryConfiguration messageSender) {
        this.messageSender = messageSender;
        return this;
    }

    public ClientStoreConfiguration getClientStore() {
        return clientStore;
    }

    public ClientChannelConfigurationBuilder setClientStore(ClientStoreConfiguration clientStore) {
        this.clientStore = clientStore;
        return this;
    }

    public ClientChannelConfiguration toConfiguration() {
        return new ClientChannelConfiguration(endpoint, group, messageSender, clientStore);
    }
}
