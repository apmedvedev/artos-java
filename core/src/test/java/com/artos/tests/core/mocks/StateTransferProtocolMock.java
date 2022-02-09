package com.artos.tests.core.mocks;

import com.artos.impl.core.server.state.IStateTransferProtocol;
import com.exametrika.common.utils.ICompletionHandler;

public class StateTransferProtocolMock implements IStateTransferProtocol {
    private boolean stateTransferRequested;
    private Object result;
    private Throwable error;
    private String host;
    private int port;
    private long startLogIndex;
    private ICompletionHandler completionHandler;
    private boolean blockResponse;
    private boolean onLeader;
    private boolean onFollower;

    public void setResult(Object result) {
        this.result = result;
    }

    public void setError(Throwable error) {
        this.error = error;
    }

    public void setBlockResponse(boolean blockResponse) {
        this.blockResponse = blockResponse;
    }

    public boolean isStateTransferRequested() {
        return stateTransferRequested;
    }

    public ICompletionHandler getCompletionHandler() {
        return completionHandler;
    }

    public long getStartLogIndex() {
        return startLogIndex;
    }

    public boolean isOnFollower() {
        return onFollower;
    }

    public boolean isOnLeader() {
        return onLeader;
    }

    public void reset() {
        stateTransferRequested = false;
    }

    @Override
    public void requestStateTransfer(String host, int port, long startLogIndex, ICompletionHandler completionHandler) {
        this.host = host;
        this.port = port;
        this.startLogIndex = startLogIndex;
        this.completionHandler = completionHandler;
        stateTransferRequested = true;

        if (!blockResponse && completionHandler != null) {
            if (error == null)
                completionHandler.onSucceeded(result);
            else
                completionHandler.onFailed(error);
        }
    }

    @Override
    public void onLeader() {
        onLeader = true;
    }

    @Override
    public void onFollower() {
        onFollower = true;
    }

    @Override
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setStartLogIndex(long startLogIndex) {
        this.startLogIndex = startLogIndex;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
}
