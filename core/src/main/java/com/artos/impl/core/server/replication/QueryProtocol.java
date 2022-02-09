package com.artos.impl.core.server.replication;

import com.artos.impl.core.message.QueryRequest;
import com.artos.impl.core.message.QueryRequestItem;
import com.artos.impl.core.message.QueryResponse;
import com.artos.impl.core.message.QueryResponseBuilder;
import com.artos.impl.core.message.QueryResponseItem;
import com.artos.impl.core.message.SubscriptionRequest;
import com.artos.impl.core.message.SubscriptionRequestItem;
import com.artos.impl.core.message.SubscriptionResponse;
import com.artos.impl.core.message.SubscriptionResponseBuilder;
import com.artos.impl.core.message.SubscriptionResponseItem;
import com.artos.impl.core.server.impl.Context;
import com.artos.spi.core.IStateMachineTransaction;
import com.exametrika.common.compartment.ICompartmentTimer;
import com.exametrika.common.compartment.impl.CompartmentTimer;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.CompletionHandler;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.SimpleDeque;
import com.exametrika.common.utils.Times;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class QueryProtocol implements IQueryProtocol {
    private final Context context;
    private final ClientSessionManager clientSessionManager;
    private final ICompartmentTimer subscriptionTimeoutTimer = new CompartmentTimer((currentTime) -> handleSubscriptionTimeout());

    public QueryProtocol(Context context, ClientSessionManager clientSessionManager) {
        Assert.notNull(context);
        Assert.notNull(clientSessionManager);

        this.context = context;
        this.clientSessionManager = clientSessionManager;
        subscriptionTimeoutTimer.setPeriod(context.getServerChannelFactoryConfiguration().getSubscriptionTimeout());
    }

    @Override
    public void handleQueryRequest(QueryRequest request, ICompletionHandler<QueryResponse> responseHandler) {
        ClientSession session = clientSessionManager.ensureSession(request.getSource());

        if (request.getQueries() != null) {
            for (QueryRequestItem query : request.getQueries()) {
                long lastPublishedMessageId = query.getLastPublishedMessageId();
                QueryInfo queryInfo = new QueryInfo(query, responseHandler);
                if (lastPublishedMessageId > session.getLastCommittedMessageId())
                    session.addQuery(queryInfo);
                else {
                    processQuery(queryInfo);
                }
            }
        } else {
            QueryResponseBuilder builder = new QueryResponseBuilder();
            builder.setAccepted(true);
            builder.setGroupId(context.getGroupId());
            builder.setSource(context.getServerId());

            responseHandler.onSucceeded(builder.toMessage());
        }
    }

    @Override
    public void handleSubscriptionRequest(SubscriptionRequest request, ICompletionHandler<SubscriptionResponse> responseHandler) {
        ClientSession session = clientSessionManager.ensureSession(request.getSource());

        if (request.getSubscriptions() != null) {
            Set<UUID> subscriptionIds = new HashSet<>();
            for (SubscriptionRequestItem subscription : request.getSubscriptions()) {
                subscriptionIds.add(subscription.getSubscriptionId());
            }

            Set<UUID> removedSubscriptionIds = new HashSet<>();
            for (UUID subscription : session.getSubscriptions()) {
                if (responseHandler.isCanceled() || !subscriptionIds.contains(subscription))
                    removedSubscriptionIds.add(subscription);
            }

            session.setSubscriptions(subscriptionIds);
            processSubscriptions(request, responseHandler, removedSubscriptionIds);
        }
    }

    @Override
    public void onSessionRemoved(ClientSession session) {
        if (!session.getSubscriptions().isEmpty())
            processSubscriptions(null, null, session.getSubscriptions());
    }

    @Override
    public void onCommit(ClientSession session) {
        SimpleDeque<QueryInfo> queryQueue = session.getQueryQueue();
        while (!queryQueue.isEmpty()) {
            QueryInfo queryInfo = queryQueue.peek();
            long lastPublishedMessageId = queryInfo.getQuery().getLastPublishedMessageId();
            if (lastPublishedMessageId > session.getLastCommittedMessageId())
                break;

            queryQueue.poll();
            processQuery(queryInfo);
        }
    }

    @Override
    public void onFollower() {
        for (ClientSession session : clientSessionManager.getSessions().values())
            session.getQueryQueue().clear();
    }

    @Override
    public void onTimer(long currentTime) {
        subscriptionTimeoutTimer.onTimer(currentTime);
    }

    @Override
    public void start() {
        subscriptionTimeoutTimer.start();
    }

    @Override
    public void stop() {
        subscriptionTimeoutTimer.stop();
    }

    private void processQuery(QueryInfo queryInfo) {
        if (queryInfo.getResponseHandler().isCanceled())
            return;

        IStateMachineTransaction transaction = context.getStateMachine().beginTransaction(true);
        transaction.query(queryInfo.getQuery().getValue(), new CompletionHandler<ByteArray>() {
            @Override
            public boolean isCanceled() {
                return queryInfo.getResponseHandler().isCanceled();
            }

            @Override
            public void onSucceeded(ByteArray result) {
                QueryResponseBuilder builder = new QueryResponseBuilder();
                builder.setAccepted(true);
                builder.setGroupId(context.getGroupId());
                builder.setSource(context.getServerId());
                builder.setResults(Collections.singletonList(new QueryResponseItem(queryInfo.getQuery().getMessageId(),
                    result)));

                queryInfo.getResponseHandler().onSucceeded(builder.toMessage());
            }

            @Override
            public void onFailed(Throwable error) {
                queryInfo.getResponseHandler().onFailed(error);
            }
        });
        transaction.commit();
    }

    private void processSubscriptions(SubscriptionRequest request, ICompletionHandler<SubscriptionResponse> responseHandler,
                                      Set<UUID> removedSubscriptionIds) {
        IStateMachineTransaction transaction = context.getStateMachine().beginTransaction(true);
        for (UUID subscriptionId : removedSubscriptionIds)
            transaction.unsubscribe(subscriptionId);

        if (request != null) {
            for (SubscriptionRequestItem subscription : request.getSubscriptions()) {
                transaction.subscribe(subscription.getSubscriptionId(), subscription.getValue(), new CompletionHandler<ByteArray>() {
                    @Override
                    public boolean isCanceled() {
                        return responseHandler.isCanceled();
                    }

                    @Override
                    public void onSucceeded(ByteArray result) {
                        SubscriptionResponseBuilder builder = new SubscriptionResponseBuilder();
                        builder.setAccepted(true);
                        builder.setGroupId(context.getGroupId());
                        builder.setSource(context.getServerId());
                        builder.setResults(Collections.singletonList(new SubscriptionResponseItem(subscription.getSubscriptionId(),
                                result)));

                        responseHandler.onSucceeded(builder.toMessage());
                    }

                    @Override
                    public void onFailed(Throwable error) {
                        responseHandler.onFailed(error);
                    }
                });
            }
        }

        transaction.commit();
    }

    private void handleSubscriptionTimeout() {
        long currentTime = Times.getCurrentTime();
        for (ClientSession session : clientSessionManager.getSessions().values()) {
            if (currentTime >= session.getLastUpdateSubscriptionTime() + context.getServerChannelFactoryConfiguration().getSubscriptionTimeout()) {
                IStateMachineTransaction transaction = context.getStateMachine().beginTransaction(true);
                for (UUID subscriptionId : session.getSubscriptions())
                    transaction.unsubscribe(subscriptionId);

                transaction.commit();

                session.setSubscriptions(Collections.emptySet());
            }
        }
    }
}
