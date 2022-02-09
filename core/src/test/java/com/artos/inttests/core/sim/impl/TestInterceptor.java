package com.artos.inttests.core.sim.impl;

import com.artos.impl.core.server.impl.Context;
import com.artos.inttests.core.sim.intercept.SimGroupInterceptors;
import com.exametrika.api.instrument.IJoinPoint;
import com.exametrika.common.l10n.NonLocalizedMessage;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.tests.Tests;
import com.exametrika.common.utils.Exceptions;
import com.exametrika.common.utils.Times;

import java.text.MessageFormat;

public class TestInterceptor {
    private final ILogger logger = Loggers.get(getClass());
    private static TestInterceptor interceptor = new TestInterceptor();
    private volatile long lastCreateLeaderRequestTime;
    private volatile long lastAppendLeaderRequestTime;
    private volatile long lastRestartElectionTime;
    public static Object onEnter(int index, int version, Object instance, Object[] params) {
        IJoinPoint joinPoint = SimGroupInterceptors.getInterceptorAllocator().getJoinPoints().get(index);
        //TODO:interceptor.intercept(joinPoint, instance);
        return null;
    }

    private void intercept(IJoinPoint joinPoint, Object instance) {
        long currentTime = Times.getCurrentTime();
        if (joinPoint.getMethodName().equals("createAppendEntriesRequest")) {
            lastCreateLeaderRequestTime = currentTime;
        } if (joinPoint.getMethodName().equals("requestAppendEntries")) {
            lastAppendLeaderRequestTime = currentTime;
        } else if (joinPoint.getMethodName().equals("restartElectionTimer")) {
            lastRestartElectionTime = currentTime;
        } else if (joinPoint.getMethodName().equals("becomeCandidate")) {



            long delta = currentTime - lastRestartElectionTime;
            long delta2 = currentTime - lastCreateLeaderRequestTime;
            long delta3 = currentTime - lastAppendLeaderRequestTime;
            logger.log(LogLevel.INFO, Loggers.getMarker(getContext(instance).getLocalServer().getEndpoint()), new NonLocalizedMessage(MessageFormat.format(
                "========= delta1: {0}, delta2: {1}, delta3: {2}", delta, delta2, delta3)));
        } else if (joinPoint.getMethodName().equals("becomeFollower")) {
            logger.log(LogLevel.INFO, Loggers.getMarker(getContext(instance).getLocalServer().getEndpoint()),
                new NonLocalizedMessage("========= becomeFollower"), new RuntimeException("becomeFollower"));
        }
    }

    private Context getContext(Object instance) {
        try {
            return Tests.get(instance, "context");
        } catch (Exception e) {
            return Exceptions.wrapAndThrow(e);
        }
    }
}
