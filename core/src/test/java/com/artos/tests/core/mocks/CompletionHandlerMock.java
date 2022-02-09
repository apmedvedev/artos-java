package com.artos.tests.core.mocks;

import com.exametrika.common.utils.ICompletionHandler;

public class CompletionHandlerMock implements ICompletionHandler {
    private Object succeeded;
    private Throwable failed;
    private boolean canceled;

    public Object getSucceeded() {
        return succeeded;
    }

    public Throwable getFailed() {
        return failed;
    }

    public void setCanceled(boolean canceled) {
        this.canceled = canceled;
    }

    public void reset() {
        succeeded = null;
        failed = null;
    }

    @Override
    public boolean isCanceled() {
        return canceled;
    }

    @Override
    public void onSucceeded(Object result) {
        if (result == null)
            result = new Object();
        this.succeeded = result;
    }

    @Override
    public void onFailed(Throwable error) {
        this.failed = error;
    }
}
