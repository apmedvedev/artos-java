package com.artos.impl.core.client;

import com.artos.api.core.client.IClientChannel;
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

import java.util.UUID;

public class ClientChannel implements IClientChannel {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final IClientProtocol clientProtocol;
    private final ICompartment compartment;
    private volatile boolean started;

    public ClientChannel(String endpoint, IClientProtocol clientProtocol, ICompartment compartment) {
        Assert.notNull(clientProtocol);
        Assert.notNull(compartment);

        this.clientProtocol = clientProtocol;
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
    public void publish(ByteArray value, ICompletionHandler<ByteArray> commitHandler) {
        compartment.offer(() -> clientProtocol.getPublishProtocol().publish(value, commitHandler));
    }

    @Override
    public void query(ByteArray request, ICompletionHandler<ByteArray> responseHandler) {
        compartment.offer(() -> clientProtocol.getQueryProtocol().query(request, responseHandler));
    }

    @Override
    public void subscribe(UUID subscriptionId, ByteArray request, ICompletionHandler<ByteArray> responseHandler) {
        compartment.offer(() -> clientProtocol.getQueryProtocol().subscribe(subscriptionId, request, responseHandler));
    }

    @Override
    public void unsubscribe(UUID subscriptionId) {
        compartment.offer(() -> clientProtocol.getQueryProtocol().unsubscribe(subscriptionId));
    }

    private interface IMessages {
        @DefaultMessage("Client channel has been started.")
        ILocalizedMessage channelStarted();

        @DefaultMessage("Client channel has been stopped.")
        ILocalizedMessage channelStopped();
    }
}
