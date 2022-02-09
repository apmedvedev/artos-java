package com.artos.inttests.core.sim.impl;

import com.artos.inttests.core.sim.api.ISimGroupLinks;
import com.artos.inttests.core.sim.model.SimLink;
import com.artos.inttests.core.sim.model.SimRouter;
import com.artos.inttests.core.sim.perf.IMeter;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SimGroupLinks implements ISimGroupLinks {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker = Loggers.getMarker("model");
    private final Map<String, SimLink> links = new LinkedHashMap<>();
    private final List<SimLink> enabledLinks = new ArrayList<>();
    private final Random random = new Random();
    private final IMeter failedLinksMeter;
    private final SimRouter router;

    public SimGroupLinks(PerfRegistry perfRegistry) {
        router = new SimRouter(links, links);
        failedLinksMeter = perfRegistry.getMeter("model:total.failed-links(counts)");

    }

    public SimRouter getRouter() {
        return router;
    }

    public synchronized SimLink addLink(String endpoint) {
        SimLink link = new SimLink(router, endpoint);
        synchronized (links) {
            links.put(endpoint, link);
        }
        enabledLinks.add(link);

        return link;
    }

    public synchronized void removeLink(String endpoint) {
        SimLink link;
        synchronized (links) {
            link = links.remove(endpoint);
        }
        enabledLinks.remove(link);
    }

    @Override
    public synchronized void enableAllLinks() {
        enabledLinks.clear();
        synchronized (links) {
            for (SimLink link : links.values()) {
                link.setEnabled(true);
                enabledLinks.add(link);
            }
        }

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.allLinksEnabled());
    }

    public synchronized void disableLink(String endpoint) {
        SimLink link;
        synchronized (links) {
            link = links.get(endpoint);
        }
        link.setEnabled(false);
        enabledLinks.remove(link);

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.linkDisabled(link.getEndpoint()));

        failedLinksMeter.measure(1);
    }

    @Override
    public synchronized void disableRandomLink() {
        if (enabledLinks.isEmpty())
            return;

        int index = random.nextInt(enabledLinks.size());
        SimLink link = enabledLinks.remove(index);
        link.setEnabled(false);

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.linkDisabled(link.getEndpoint()));

        failedLinksMeter.measure(1);
    }

    @Override
    public synchronized void setRandomLinkLatency(long latency) {
        if (enabledLinks.isEmpty())
            return;

        int index = random.nextInt(enabledLinks.size());
        SimLink link = enabledLinks.get(index);
        link.setLatency(latency);

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.linkLatencyChanged(link.getEndpoint(), latency));
    }

    @Override
    public synchronized void setAllLinksLatency(long latency) {
        synchronized (links) {
            for (SimLink link : links.values()) {
                link.setLatency(latency);
                if (logger.isLogEnabled(LogLevel.INFO))
                    logger.log(LogLevel.INFO, marker, messages.linkLatencyChanged(link.getEndpoint(), latency));
            }
        }
    }

    private interface IMessages {
        @DefaultMessage("All links are enabled.")
        ILocalizedMessage allLinksEnabled();

        @DefaultMessage("Link is disabled: {0}.")
        ILocalizedMessage linkDisabled(String endpoint);

        @DefaultMessage("Link latency has been changed. Link: {0}, latency(ms): {1}.")
        ILocalizedMessage linkLatencyChanged(String endpoint, long latency);
    }
}
