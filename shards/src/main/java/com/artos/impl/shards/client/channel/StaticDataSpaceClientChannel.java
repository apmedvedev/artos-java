package com.artos.impl.shards.client.channel;

import com.artos.api.shards.client.IDataSpaceClientChannel;
import com.artos.impl.core.client.IClientProtocol;
import com.exametrika.common.compartment.ICompartment;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.SyncCompletionHandler;

import java.util.List;
import java.util.UUID;
import java.util.function.ToIntFunction;

public class StaticDataSpaceClientChannel<K> implements IDataSpaceClientChannel<K> {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final List<IClientProtocol> clientProtocols;
    private final ICompartment compartment;
    private final ToIntFunction<K> hashFunction;
    private volatile boolean started;

    public StaticDataSpaceClientChannel(String endpoint, List<IClientProtocol> clientProtocols, ToIntFunction<K> hashFunction,
                                        ICompartment compartment) {
        Assert.notNull(clientProtocols);
        Assert.notNull(hashFunction);
        Assert.notNull(compartment);

        this.clientProtocols = clientProtocols;
        this.hashFunction = hashFunction;
        this.compartment = compartment;
        marker = Loggers.getMarker(endpoint);
    }

    @Override
    public void start() {
        compartment.start();

        try {
            SyncCompletionHandler completionHandler = new SyncCompletionHandler();
            compartment.offer(() -> {
                try {
                    for (IClientProtocol clientProtocol : clientProtocols)
                        clientProtocol.start();
                    completionHandler.onSucceeded(true);
                } catch (Exception e) {
                    completionHandler.onFailed(e);
                }
            });
            completionHandler.await(Integer.MAX_VALUE);
        } catch (Exception e) {
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);
        }

        started = true;

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.channelStarted());
    }

    @Override
    public void stop() {
        try {
            SyncCompletionHandler completionHandler = new SyncCompletionHandler();
            compartment.offer(() -> {
                try {
                    for (IClientProtocol clientProtocol : clientProtocols)
                        clientProtocol.stop();
                    completionHandler.onSucceeded(true);
                } catch (Exception e) {
                    completionHandler.onFailed(e);
                }
            });
            completionHandler.await(Integer.MAX_VALUE);
        } catch (Exception e) {
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);
        }

        compartment.stop();
        started = false;

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.channelStopped());
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public void query(K key, ByteArray request, ICompletionHandler<ByteArray> responseHandler) {
        IClientProtocol clientProtocol = getClientProtocol(key);
        compartment.offer(() -> clientProtocol.getQueryProtocol().query(request, responseHandler));
    }

    @Override
    public void subscribe(K key, UUID subscriptionId, ByteArray request, ICompletionHandler<ByteArray> responseHandler) {
        IClientProtocol clientProtocol = getClientProtocol(key);
        compartment.offer(() -> clientProtocol.getQueryProtocol().subscribe(subscriptionId, request, responseHandler));
    }

    @Override
    public void unsubscribe(K key, UUID subscriptionId) {
        IClientProtocol clientProtocol = getClientProtocol(key);
        compartment.offer(() -> clientProtocol.getQueryProtocol().unsubscribe(subscriptionId));
    }

    @Override
    public void publish(K key, ByteArray value, ICompletionHandler<ByteArray> commitHandler) {
        IClientProtocol clientProtocol = getClientProtocol(key);
        compartment.offer(() -> clientProtocol.getPublishProtocol().publish(value, commitHandler));
    }

    private IClientProtocol getClientProtocol(K key) {
        int index = Math.abs(hashFunction.applyAsInt(key)) % clientProtocols.size();
        return clientProtocols.get(index);
    }

    private interface IMessages {
        @DefaultMessage("Сhannel has been started.")
        ILocalizedMessage channelStarted();

        @DefaultMessage("Сhannel has been stopped.")
        ILocalizedMessage channelStopped();
    }
}
