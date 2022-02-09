package com.artos.inttests.core.sim.model;

import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.l10n.SystemException;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;

import java.util.Map;

public class SimRouter {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final Map<String, SimLink> links;
    private final Object lock;

    public SimRouter(Map<String, SimLink> links, Object lock) {
        this.marker = Loggers.getMarker("router");
        this.links = links;
        this.lock = lock;
    }

    public void route(String endpoint, SimMessage message) {
        SimLink link = findLink(endpoint);
        if (link != null) {
            link.offer(message);
        } else {
            message.getResponse().onFailed(new SystemException(messages.linkNotFound(endpoint)));
            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.linkNotFound(endpoint));
        }
    }

    public SimLink getLink(String endpoint) {
        synchronized (lock) {
            SimLink link = links.get(endpoint);
            if (link != null)
                return link;
            else
                throw new SystemException(messages.linkNotFound(endpoint));
        }
    }

    public SimLink findLink(String endpoint) {
        synchronized (lock) {
            return links.get(endpoint);
        }
    }

    private interface IMessages {
        @DefaultMessage("Link is not found: {0}.")
        ILocalizedMessage linkNotFound(String endpoint);
    }
}
