package com.artos.api.core.server.conf;

import com.artos.spi.core.conf.LogStoreFactoryConfiguration;
import com.artos.spi.core.conf.MessageListenerFactoryConfiguration;
import com.artos.spi.core.conf.MessageSenderFactoryConfiguration;

public class ServerChannelConfigurationBuilder {
    private ServerConfiguration server;
    private GroupConfiguration group;
    private LogStoreFactoryConfiguration logStore;
    private MessageListenerFactoryConfiguration messageListener;
    private MessageSenderFactoryConfiguration messageSender;
    private Integer stateTransferPortRangeStart;
    private Integer stateTransferPortRangeEnd;
    private String stateTransferBindAddress;
    private boolean stateTransferUseCompression;
    private long stateTransferMaxSize;
    private String workDir;
    private boolean secured;
    private String keyStoreFile;
    private String keyStorePassword;

    public ServerConfiguration getServer() {
        return server;
    }

    public ServerChannelConfigurationBuilder setServer(ServerConfiguration server) {
        this.server = server;
        return this;
    }

    public GroupConfiguration getGroup() {
        return group;
    }

    public ServerChannelConfigurationBuilder setGroup(GroupConfiguration group) {
        this.group = group;
        return this;
    }

    public LogStoreFactoryConfiguration getLogStore() {
        return logStore;
    }

    public ServerChannelConfigurationBuilder setLogStore(LogStoreFactoryConfiguration logStore) {
        this.logStore = logStore;
        return this;
    }

    public MessageListenerFactoryConfiguration getMessageListener() {
        return messageListener;
    }

    public ServerChannelConfigurationBuilder setMessageListener(MessageListenerFactoryConfiguration messageListener) {
        this.messageListener = messageListener;
        return this;
    }

    public MessageSenderFactoryConfiguration getMessageSender() {
        return messageSender;
    }

    public ServerChannelConfigurationBuilder setMessageSender(MessageSenderFactoryConfiguration messageSender) {
        this.messageSender = messageSender;
        return this;
    }

    public Integer getStateTransferPortRangeStart() {
        return stateTransferPortRangeStart;
    }

    public ServerChannelConfigurationBuilder setStateTransferPortRangeStart(Integer stateTransferPortRangeStart) {
        this.stateTransferPortRangeStart = stateTransferPortRangeStart;
        return this;
    }

    public Integer getStateTransferPortRangeEnd() {
        return stateTransferPortRangeEnd;
    }

    public ServerChannelConfigurationBuilder setStateTransferPortRangeEnd(Integer stateTransferPortRangeEnd) {
        this.stateTransferPortRangeEnd = stateTransferPortRangeEnd;
        return this;
    }

    public String getStateTransferBindAddress() {
        return stateTransferBindAddress;
    }

    public ServerChannelConfigurationBuilder setStateTransferBindAddress(String stateTransferBindAddress) {
        this.stateTransferBindAddress = stateTransferBindAddress;
        return this;
    }

    public boolean isStateTransferUseCompression() {
        return stateTransferUseCompression;
    }

    public ServerChannelConfigurationBuilder setStateTransferUseCompression(boolean stateTransferUseCompression) {
        this.stateTransferUseCompression = stateTransferUseCompression;
        return this;
    }

    public long getStateTransferMaxSize() {
        return stateTransferMaxSize;
    }

    public ServerChannelConfigurationBuilder setStateTransferMaxSize(long stateTransferMaxSize) {
        this.stateTransferMaxSize = stateTransferMaxSize;
        return this;
    }

    public String getWorkDir() {
        return workDir;
    }

    public ServerChannelConfigurationBuilder setWorkDir(String workDir) {
        this.workDir = workDir;
        return this;
    }

    public boolean isSecured() {
        return secured;
    }

    public ServerChannelConfigurationBuilder setSecured(boolean secured) {
        this.secured = secured;
        return this;
    }

    public String getKeyStoreFile() {
        return keyStoreFile;
    }

    public ServerChannelConfigurationBuilder setKeyStoreFile(String keyStoreFile) {
        this.keyStoreFile = keyStoreFile;
        return this;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public ServerChannelConfigurationBuilder setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public ServerChannelConfiguration toConfiguration() {
        return new ServerChannelConfiguration(server, group, logStore, messageListener, messageSender,
            stateTransferPortRangeStart, stateTransferPortRangeEnd,
            stateTransferBindAddress, stateTransferUseCompression, stateTransferMaxSize,
            workDir, secured, keyStoreFile, keyStorePassword);
    }
}
