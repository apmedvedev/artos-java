package com.artos.impl.core.server.state;

import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.l10n.SystemException;
import com.exametrika.common.utils.ICompletionHandler;

public class NoOpStateTransferProtocol implements IStateTransferProtocol {
    private static final IMessages messages = Messages.get(IMessages.class);

    @Override
    public void requestStateTransfer(String host, int port, long startLogIndex, ICompletionHandler completionHandler) {
        completionHandler.onFailed(new SystemException(messages.stateTransferNotSupported()));
    }

    @Override
    public void onLeader() {
    }

    @Override
    public void onFollower() {
    }

    @Override
    public String getHost() {
        return null;
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    private interface IMessages {
        @DefaultMessage("State transfer is not supported in single server group.")
        ILocalizedMessage stateTransferNotSupported();
    }
}
