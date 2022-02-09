/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.client;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.ClientRequest;
import com.artos.impl.core.message.ClientResponse;
import com.artos.impl.core.message.Message;
import com.artos.impl.core.message.QueryRequest;
import com.artos.impl.core.message.QueryRequestBuilder;
import com.artos.impl.core.message.QueryRequestItem;
import com.artos.impl.core.message.QueryResponse;
import com.artos.impl.core.message.QueryResponseItem;
import com.artos.impl.core.message.SubscriptionRequestBuilder;
import com.artos.impl.core.message.SubscriptionRequestItem;
import com.artos.impl.core.message.SubscriptionResponse;
import com.artos.impl.core.message.SubscriptionResponseItem;
import com.artos.impl.core.server.utils.MessageUtils;
import com.artos.spi.core.FlowType;
import com.artos.spi.core.IMessageSender;
import com.artos.spi.core.IMessageSenderFactory;
import com.exametrika.common.compartment.ICompartment;
import com.exametrika.common.compartment.ICompartmentProcessor;
import com.exametrika.common.compartment.ICompartmentTimer;
import com.exametrika.common.compartment.ICompartmentTimerProcessor;
import com.exametrika.common.compartment.impl.CompartmentTimer;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.tasks.IFlowController;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.CompletionHandler;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.SimpleDeque;
import com.exametrika.common.utils.SimpleList;
import com.exametrika.common.utils.SimpleList.Element;
import com.exametrika.common.utils.Times;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class QueryClientProtocol implements IQueryClientProtocol, ICompartmentProcessor, ICompartmentTimerProcessor {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final IMessageSenderFactory messageSenderFactory;
    private final UUID clientId;
    private final UUID groupId;
    private final int lockQueueCapacity;
    private final int unlockQueueCapacity;
    private final int maxValueSize;
    private final int maxBatchSize;
    private final long resendQueryPeriod;
    private final Random random = new Random();
    private ICompartment compartment;
    private IClientProtocol clientProtocol;
    private IPublishClientProtocol publishProtocol;
    private IFlowController<FlowType> flowController;
    private final ICompartmentTimer serverTimeoutTimer = new CompartmentTimer((currentTime) -> handleServerTimeout());
    private final ICompartmentTimer heartbeatTimer = new CompartmentTimer((currentTime) -> requestHeartbeat());
    private final ICompartmentTimer resubscriptionTimer = new CompartmentTimer((currentTime) -> resubscribe());
    private final SimpleDeque<QueryInfo> pendingQueries = new SimpleDeque<>();
    private final SimpleList<QueryInfo> sentQueries = new SimpleList<>();
    private final TLongObjectMap<QueryInfo> sentQueriesMap = new TLongObjectHashMap<>();
    private final Map<UUID, SubscriptionInfo> subscriptions = new LinkedHashMap<>();
    private long nextMessageId = 1;
    private boolean flowLocked;
    private GroupConfiguration configuration;
    private IMessageSender messageSender;
    private ServerConfiguration serverConfiguration;
    private boolean resubscriptionRequired;

    public QueryClientProtocol(IMessageSenderFactory messageSenderFactory, String endpoint, UUID clientId, UUID groupId,
                               int lockQueueCapacity, int unlockQueueCapacity, int maxValueSize, int maxBatchSize,
                               long serverTimeout, long heartbeatPeriod, long resendQueryPeriod, long resubscriptionPeriod) {
        Assert.notNull(messageSenderFactory);
        Assert.notNull(clientId);
        Assert.notNull(groupId);
        Assert.isTrue(maxBatchSize >= maxValueSize);

        this.messageSenderFactory = messageSenderFactory;
        this.clientId = clientId;
        this.groupId = groupId;
        this.lockQueueCapacity = lockQueueCapacity;
        this.unlockQueueCapacity = unlockQueueCapacity;
        this.maxValueSize = maxValueSize;
        this.maxBatchSize = maxBatchSize;
        this.serverTimeoutTimer.setPeriod(serverTimeout);
        this.heartbeatTimer.setPeriod(heartbeatPeriod);
        this.resendQueryPeriod = resendQueryPeriod;
        this.resubscriptionTimer.setPeriod(resubscriptionPeriod);
        this.marker = Loggers.getMarker(endpoint);
    }

    public void setCompartment(ICompartment compartment) {
        Assert.notNull(compartment);
        Assert.isNull(this.compartment);

        this.compartment = compartment;
    }

    public void setClientProtocol(IClientProtocol clientProtocol) {
        Assert.notNull(clientProtocol);
        Assert.isNull(this.clientProtocol);

        this.clientProtocol = clientProtocol;
    }

    public void setPublishProtocol(IPublishClientProtocol publishProtocol) {
        Assert.notNull(publishProtocol);
        Assert.isNull(this.publishProtocol);

        this.publishProtocol = publishProtocol;
    }

    public void setFlowController(IFlowController<FlowType> flowController) {
        Assert.notNull(flowController);
        Assert.isNull(this.flowController);

        this.flowController = flowController;
    }

    @Override
    public void start() {
        if (!configuration.isSingleServer())
            heartbeatTimer.start();

        resubscriptionTimer.start();
    }

    @Override
    public void stop() {
        onServerDisconnected(false);
        heartbeatTimer.stop();
        resubscriptionTimer.stop();
    }

    @Override
    public void process() {
        if (messageSender != null) {
            QueryRequest request = createRequest();
            if (request != null)
                sendRequest(request);
        }
    }

    @Override
    public void onTimer(long currentTime) {
        serverTimeoutTimer.onTimer(currentTime);
        heartbeatTimer.onTimer(currentTime);
        resubscriptionTimer.onTimer(currentTime);

        if (resubscriptionRequired && messageSender != null) {
            resubscribe();
            resubscriptionRequired = false;
        }
    }

    @Override
    public void query(ByteArray request, ICompletionHandler<ByteArray> responseHandler) {
        Assert.notNull(request);
        Assert.notNull(responseHandler);
        Assert.isTrue(request.getLength() <= maxValueSize);

        long lastPublishedMessageId = publishProtocol.getNextMessageId() - 1;
        QueryRequestItem query = new QueryRequestItem(lastPublishedMessageId, nextMessageId, request);
        nextMessageId++;
        pendingQueries.offer(new QueryInfo(query, responseHandler));

        checkFlow();
    }

    @Override
    public void subscribe(UUID subscriptionId, ByteArray request, ICompletionHandler<ByteArray> responseHandler) {
        Assert.notNull(subscriptionId);
        Assert.notNull(request);
        Assert.notNull(responseHandler);
        Assert.isTrue(request.getLength() <= maxValueSize);

        SubscriptionRequestItem subscription = new SubscriptionRequestItem(subscriptionId, request);
        SubscriptionInfo info = new SubscriptionInfo(subscription, responseHandler);
        subscriptions.put(subscriptionId, info);

        resubscriptionRequired = true;
    }

    @Override
    public void unsubscribe(UUID subscriptionId) {
        Assert.notNull(subscriptionId);
        if (subscriptions.remove(subscriptionId) != null)
            resubscriptionRequired = true;
    }

    @Override
    public void updateConfiguration(GroupConfiguration configuration) {
        if (!configuration.equals(this.configuration)) {
            this.configuration = configuration;

            if (serverConfiguration != null && configuration.findServer(serverConfiguration.getId()) == null)
                onServerDisconnected(false);
        }
    }

    @Override
    public void onLeaderConnected(IMessageSender messageSender, ServerConfiguration serverConfiguration) {
        if (configuration.isSingleServer()) {
            this.messageSender = messageSender;
            this.serverConfiguration = serverConfiguration;
        }
    }

    @Override
    public void onLeaderDisconnected() {
        onServerDisconnected(false);
    }

    private void sendRequest(ClientRequest request) {
        IMessageSender messageSender = ensureMessageSender();
        if (messageSender == null)
            return;

        if (logger.isLogEnabled(LogLevel.TRACE))
            logger.log(LogLevel.TRACE, marker, messages.requestSent(request));

        if (!configuration.isSingleServer())
            heartbeatTimer.restart();

        ByteArray serializedRequest = MessageUtils.write(request);

        messageSender.send(serializedRequest);
    }

    private void handleSucceededResponse(ServerConfiguration serverConfiguration, ClientResponse response) {
        if (response instanceof QueryResponse)
            handleQueryResponse(serverConfiguration, (QueryResponse) response);
        else if (response instanceof SubscriptionResponse)
            handleSubscriptionResponse(serverConfiguration, (SubscriptionResponse) response);
        else
            Assert.error();
    }

    private void handleSubscriptionResponse(ServerConfiguration serverConfiguration, SubscriptionResponse response) {
        if (!response.getSource().equals(serverConfiguration.getId()))
            return;

        if (logger.isLogEnabled(LogLevel.TRACE))
            logger.log(LogLevel.TRACE, marker, messages.responseReceived(response));

        if (response.isAccepted()) {
            if (response.getResults() != null) {
                for (SubscriptionResponseItem result : response.getResults()) {
                    SubscriptionInfo info = subscriptions.get(result.getSubscriptionId());
                    if (info != null)
                        info.resultHandler.onSucceeded(result.getValue());
                }
            }

            if (!configuration.isSingleServer())
                serverTimeoutTimer.restart();
        } else {
            onServerDisconnected(true);
        }
    }

    private void handleQueryResponse(ServerConfiguration serverConfiguration, QueryResponse response) {
        if (!response.getSource().equals(serverConfiguration.getId()))
            return;

        if (logger.isLogEnabled(LogLevel.TRACE))
            logger.log(LogLevel.TRACE, marker, messages.responseReceived(response));

        if (response.isAccepted()) {
            if (response.getResults() != null) {
                for (QueryResponseItem result : response.getResults()) {
                    QueryInfo info = sentQueriesMap.remove(result.getMessageId());
                    if (info != null) {
                        info.element.remove();
                        info.resultHandler.onSucceeded(result.getValue());
                    }
                }
            }

            if (!configuration.isSingleServer())
                serverTimeoutTimer.restart();

            checkFlow();
        } else {
            onServerDisconnected(true);
        }
    }

    private void handleFailedResponse(ServerConfiguration serverConfiguration, Throwable error) {
        if (logger.isLogEnabled(LogLevel.WARNING))
            logger.log(LogLevel.WARNING, marker, messages.responseErrorReceived(serverConfiguration.getEndpoint()), error);

        onServerDisconnected(true);
    }

    private void requestHeartbeat() {
        QueryRequestBuilder request = new QueryRequestBuilder();
        request.setSource(clientId);
        request.setGroupId(groupId);

        sendRequest(request.toMessage());
    }

    private QueryRequest createRequest() {
        long currentTime = Times.getCurrentTime();

        List<QueryRequestItem> queries = null;
        int batchSize = 0;
        while (!pendingQueries.isEmpty()) {
            QueryInfo info = pendingQueries.peek();
            QueryRequestItem query = info.query;
            if (query.getValue().getLength() + batchSize > maxBatchSize)
                break;

            pendingQueries.poll();
            if (queries == null)
                queries = new ArrayList<>();

            queries.add(query);
            batchSize += query.getValue().getLength();

            sentQueries.addLast(info.element);
            sentQueriesMap.put(query.getMessageId(), info);
            info.lastSentTime = currentTime;
        }

        while (!sentQueries.isEmpty()) {
            QueryInfo info = sentQueries.getFirst().getValue();
            if (currentTime < info.lastSentTime + resendQueryPeriod)
                break;

            QueryRequestItem query = info.query;
            if (query.getValue().getLength() + batchSize > maxBatchSize)
                break;

            if (queries == null)
                queries = new ArrayList<>();

            queries.add(query);
            batchSize += query.getValue().getLength();

            info.element.remove();
            info.element.reset();
            sentQueries.addLast(info.element);
            info.lastSentTime = currentTime;
        }

        if (queries != null && !queries.isEmpty()) {
            QueryRequestBuilder request = new QueryRequestBuilder();
            request.setSource(clientId);
            request.setGroupId(groupId);
            request.setQueries(queries);

            return request.toMessage();
        } else {
            return null;
        }
    }

    private void resubscribe() {
        List<SubscriptionRequestItem> subscriptionRequestItems = new ArrayList<>();
        for (Map.Entry<UUID, SubscriptionInfo> entry : subscriptions.entrySet()) {
            subscriptionRequestItems.add(entry.getValue().subscription);
        }

        SubscriptionRequestBuilder request = new SubscriptionRequestBuilder();
        request.setSource(clientId);
        request.setGroupId(groupId);
        request.setSubscriptions(subscriptionRequestItems);

        sendRequest(request.toMessage());
    }

    private void checkFlow() {
        if (flowController == null)
            return;

        if (!flowLocked && pendingQueries.size() + sentQueriesMap.size() >= lockQueueCapacity) {
            flowLocked = true;
            flowController.lockFlow(FlowType.QUERY);
        }

        if (flowLocked && pendingQueries.size() + sentQueriesMap.size() <= unlockQueueCapacity) {
            flowLocked = false;
            flowController.unlockFlow(FlowType.QUERY);
        }
    }

    private IMessageSender ensureMessageSender() {
        if (!clientProtocol.isConnected())
            return null;

        if (messageSender == null) {
            Assert.checkState(!configuration.isSingleServer());

            try {
                ServerConfiguration configuration = getServerConfiguration();
                this.serverConfiguration = configuration;

                messageSender = messageSenderFactory.createSender(configuration.getEndpoint(), new CompletionHandler<ByteArray>() {
                    @Override
                    public void onSucceeded(ByteArray response) {
                        Message deserializedResponse = MessageUtils.read(response);

                        compartment.offer(() -> handleSucceededResponse(configuration, (ClientResponse)deserializedResponse));
                    }

                    @Override
                    public void onFailed(Throwable error) {
                        compartment.offer(() -> handleFailedResponse(configuration, error));
                    }
                });

                messageSender.start();

                onServerConnected();
            } catch (Exception e) {
                if (logger.isLogEnabled(LogLevel.WARNING))
                    logger.log(LogLevel.WARNING, marker, e);

                onServerDisconnected(false);
                return null;
            }
        }

        return messageSender;
    }

    private void onServerConnected() {
        serverTimeoutTimer.start();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.serverConnected(serverConfiguration));
    }

    private void onServerDisconnected(boolean notifyClientProtocol) {
        if (configuration.isSingleServer()) {
            messageSender = null;

             if (notifyClientProtocol) {
                 clientProtocol.onLeaderDisconnected();
                 return;
             }
        }

        if (serverConfiguration != null) {
            if (logger.isLogEnabled(LogLevel.INFO))
                logger.log(LogLevel.INFO, marker, messages.serverDisconnected(serverConfiguration));

            serverConfiguration = null;

            stopMessageSender();

            serverTimeoutTimer.stop();

            for (QueryInfo info : sentQueries.values())
                info.lastSentTime = 0;

            resubscriptionRequired = true;
        }
    }

    private void handleServerTimeout() {
        if (logger.isLogEnabled(LogLevel.WARNING))
            logger.log(LogLevel.WARNING, marker, messages.serverTimeout(serverConfiguration));

        onServerDisconnected(false);
    }

    private void stopMessageSender() {
        if (messageSender != null) {
            messageSender.stop();
            messageSender = null;
        }
    }

    private ServerConfiguration getServerConfiguration() {
        GroupConfiguration groupConfiguration = clientProtocol.getGroupConfiguration();
        Assert.checkState(groupConfiguration != null);

        int index = random.nextInt(groupConfiguration.getServers().size());
        ServerConfiguration serverConfiguration = groupConfiguration.getServers().get(index);

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.serverSelected(serverConfiguration));

        return serverConfiguration;
    }

    private static class QueryInfo {
        private final Element<QueryInfo> element = new Element<>(this);
        private final QueryRequestItem query;
        private final ICompletionHandler<ByteArray> resultHandler;
        private long lastSentTime;

        private QueryInfo(QueryRequestItem query, ICompletionHandler<ByteArray> resultHandler) {
            this.query = query;
            this.resultHandler = resultHandler;
        }
    }

    private static class SubscriptionInfo {
        private final SubscriptionRequestItem subscription;
        private final ICompletionHandler<ByteArray> resultHandler;

        private SubscriptionInfo(SubscriptionRequestItem subscription, ICompletionHandler<ByteArray> resultHandler) {
            this.subscription = subscription;
            this.resultHandler = resultHandler;
        }
    }

    private interface IMessages {
        @DefaultMessage("Request sent: {0}.")
        ILocalizedMessage requestSent(ClientRequest request);

        @DefaultMessage("Response received: {0}.")
        ILocalizedMessage responseReceived(ClientResponse response);

        @DefaultMessage("Response error received from server ''{0}''.")
        ILocalizedMessage responseErrorReceived(String serverId);

        @DefaultMessage("Server has been selected as query group endpoint: {0}.")
        ILocalizedMessage serverSelected(ServerConfiguration serverConfiguration);

        @DefaultMessage("Query group endpoint has been connected: {0}.")
        ILocalizedMessage serverConnected(ServerConfiguration serverConfiguration);

        @DefaultMessage("Query group endpoint has been disconnected: {0}.")
        ILocalizedMessage serverDisconnected(ServerConfiguration serverConfiguration);

        @DefaultMessage("Timeout has occured, when waiting response from server ''{0}''.")
        ILocalizedMessage serverTimeout(ServerConfiguration serverConfiguration);
    }
}
