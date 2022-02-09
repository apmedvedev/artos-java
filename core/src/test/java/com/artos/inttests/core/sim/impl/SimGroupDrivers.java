package com.artos.inttests.core.sim.impl;

import com.artos.inttests.core.sim.api.ISimGroupDrivers;
import com.artos.inttests.core.sim.model.SimPublishDriver;
import com.artos.inttests.core.sim.model.SimQueryDriver;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Assert;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SimGroupDrivers implements ISimGroupDrivers {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker = Loggers.getMarker("model");
    private final List<SimPublishDriver> publishDrivers = new ArrayList<>();
    private final List<SimQueryDriver> queryDrivers = new ArrayList<>();
    protected final long publishDriverPeriod;
    protected final int publishDriverBatchSize;
    protected final long queryDriverPeriod;
    protected final int queryDriverBatchSize;
    protected final PerfRegistry perfRegistry;
    protected final boolean dummy;

    public SimGroupDrivers(long publishDriverPeriod, int publishDriverBatchSize, long queryDriverPeriod, int queryDriverBatchSize,
                           PerfRegistry perfRegistry, boolean dummy) {
        this.publishDriverPeriod = publishDriverPeriod;
        this.publishDriverBatchSize = publishDriverBatchSize;
        this.queryDriverPeriod = queryDriverPeriod;
        this.queryDriverBatchSize = queryDriverBatchSize;
        this.perfRegistry = perfRegistry;
        this.dummy = dummy;
    }

    public synchronized List<SimPublishDriver> getPublishDrivers() {
        return new ArrayList<>(publishDrivers);
    }

    public synchronized List<SimQueryDriver> getQueryDrivers() {
        return new ArrayList<>(queryDrivers);
    }

    public synchronized SimPublishDriver getPublishDriver(String endpoint) {
        for (SimPublishDriver driver : publishDrivers) {
            if (driver.getEndpoint().equals(endpoint))
                return driver;
        }

        return Assert.error();
    }

    public synchronized SimQueryDriver getQueryDriver(String endpoint) {
        for (SimQueryDriver driver : queryDrivers) {
            if (driver.getEndpoint().equals(endpoint))
                return driver;
        }

        return Assert.error();
    }

    public synchronized SimPublishDriver addPublishDriver(String endpoint, boolean client) {
        SimPublishDriver driver = createPublishDriver(endpoint, client);
        publishDrivers.add(driver);

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.publishDriverAdded(driver.getEndpoint()));

        return driver;
    }

    public synchronized void addPublishDriver(SimPublishDriver driver) {
        publishDrivers.add(driver);
        driver.start();
    }

    public synchronized void removePublishDriver(String endpoint) {
        for (Iterator<SimPublishDriver> it = publishDrivers.iterator(); it.hasNext(); ) {
            SimPublishDriver driver = it.next();
            if (driver.getEndpoint().equals(endpoint)) {
                driver.stop();
                it.remove();

                if (logger.isLogEnabled(LogLevel.INFO))
                    logger.log(LogLevel.INFO, marker, messages.publishDriverRemoved(driver.getEndpoint()));

                break;
            }
        }
    }

    public synchronized SimQueryDriver addQueryDriver(String endpoint, SimPublishDriver publishDriver) {
        SimQueryDriver driver = createQueryDriver(endpoint, publishDriver);
        queryDrivers.add(driver);

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.queryDriverAdded(driver.getEndpoint()));

        return driver;
    }

    public synchronized void addQueryDriver(SimQueryDriver driver) {
        queryDrivers.add(driver);
    }

    public synchronized void removeQueryDriver(String endpoint) {
        for (Iterator<SimQueryDriver> it = queryDrivers.iterator(); it.hasNext(); ) {
            SimQueryDriver driver = it.next();
            if (driver.getEndpoint().equals(endpoint)) {
                driver.stop();
                it.remove();

                if (logger.isLogEnabled(LogLevel.INFO))
                    logger.log(LogLevel.INFO, marker, messages.queryDriverRemoved(driver.getEndpoint()));

                break;
            }
        }
    }

    @Override
    public synchronized void enableDrivers(boolean enabled) {
        if (enabled) {
            if (logger.isLogEnabled(LogLevel.INFO))
                logger.log(LogLevel.INFO, marker, messages.driversEnabled());
        } else {
            if (logger.isLogEnabled(LogLevel.INFO))
                logger.log(LogLevel.INFO, marker, messages.driversDisabled());
        }

        for (SimPublishDriver driver : publishDrivers) {
            if (canBeEnabled(driver))
                driver.setEnabled(enabled);
        }

        for (SimQueryDriver driver : queryDrivers) {
            if (canBeEnabled(driver))
                driver.setEnabled(enabled);
        }
    }

    protected boolean canBeEnabled(SimQueryDriver driver) {
        return true;
    }

    protected boolean canBeEnabled(SimPublishDriver driver) {
        return true;
    }

    public synchronized void stop() {
        for (SimPublishDriver driver : publishDrivers)
            driver.stop();

        for (SimQueryDriver driver : queryDrivers)
            driver.stop();
    }

    protected SimPublishDriver createPublishDriver(String endpoint, boolean client) {
        return new SimPublishDriver(endpoint, publishDriverPeriod, publishDriverBatchSize, false, perfRegistry, client, dummy);
    }

    protected SimQueryDriver createQueryDriver(String endpoint, SimPublishDriver publishDriver) {
        return new SimQueryDriver(endpoint, queryDriverPeriod, queryDriverBatchSize, false, perfRegistry, publishDriver);
    }

    private interface IMessages {
        @DefaultMessage("Publish driver has been added: {0}.")
        ILocalizedMessage publishDriverAdded(String endpoint);

        @DefaultMessage("Publish driver has been removed: {0}.")
        ILocalizedMessage publishDriverRemoved(String endpoint);

        @DefaultMessage("Query driver has been added: {0}.")
        ILocalizedMessage queryDriverAdded(String endpoint);

        @DefaultMessage("Query driver has been removed: {0}.")
        ILocalizedMessage queryDriverRemoved(String endpoint);

        @DefaultMessage("Drivers are enabled.")
        ILocalizedMessage driversEnabled();

        @DefaultMessage("Drivers are disabled.")
        ILocalizedMessage driversDisabled();
    }
}
