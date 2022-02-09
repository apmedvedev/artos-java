package com.artos.impl.core.server.client;

import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.replication.IReplicationProtocol;
import com.artos.spi.core.FlowType;
import com.artos.spi.core.LogEntry;
import com.artos.spi.core.LogValueType;
import com.exametrika.common.compartment.ICompartmentTimer;
import com.exametrika.common.compartment.impl.CompartmentTimer;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.l10n.SystemException;
import com.exametrika.common.tasks.IFlowController;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.SimpleDeque;

public class LeaderClientProtocol implements ILeaderClientProtocol {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final State state;
    private final int maxValueSize;
    private final int lockQueueCapacity;
    private final int unlockQueueCapacity;
    private Context context;
    private IFlowController<FlowType> flowController;
    private IReplicationProtocol replicationProtocol;
    private final ICompartmentTimer commitTimer = new CompartmentTimer((currentTime) -> handleCommit());
    private final SimpleDeque<EntryInfo> queue = new SimpleDeque<>();
    private long nextMessageId = 1;
    private boolean flowLocked;

    public LeaderClientProtocol(State state, int maxValueSize, int lockQueueCapacity, int unlockQueueCapacity,
                                long commitPeriod) {
        Assert.notNull(state);

        this.state = state;
        this.maxValueSize = maxValueSize;
        this.lockQueueCapacity = lockQueueCapacity;
        this.unlockQueueCapacity = unlockQueueCapacity;
        commitTimer.setPeriod(commitPeriod);
    }

    public void setContext(Context context) {
        Assert.notNull(context);
        Assert.isNull(this.context);

        this.context = context;
    }

    public void setFlowController(IFlowController<FlowType> flowController) {
        Assert.notNull(flowController);
        Assert.isNull(this.flowController);

        this.flowController = flowController;
    }

    public void setReplicationProtocol(IReplicationProtocol replicationProtocol) {
        Assert.notNull(replicationProtocol);
        Assert.isNull(this.replicationProtocol);

        this.replicationProtocol = replicationProtocol;
    }

    @Override
    public void publish(ByteArray value, ICompletionHandler<ByteArray> commitHandler) {
        Assert.notNull(value);
        Assert.notNull(commitHandler);
        Assert.isTrue(value.getLength() <= maxValueSize);

        context.getCompartment().offer(() -> {
            if (state.getRole() != ServerRole.LEADER) {
                commitHandler.onFailed(new SystemException(messages.serverNotLeader()));
                return;
            }

            LogEntry logEntry = new LogEntry(0, value, LogValueType.APPLICATION, null, nextMessageId);
            nextMessageId++;

            queue.offer(new EntryInfo(logEntry, commitHandler));

            checkFlow();

            long lastCommittedMessageId = replicationProtocol.publish(logEntry);
            if (lastCommittedMessageId != 0)
                removeCommittedEntries(lastCommittedMessageId);
        });
    }

    @Override
    public void onLeader() {
        commitTimer.start();
        nextMessageId = replicationProtocol.getLastReceivedMessageId() + 1;
    }

    @Override
    public void onFollower() {
        commitTimer.stop();
        clear();
    }

    @Override
    public void onTimer(long currentTime) {
        commitTimer.onTimer(currentTime);
    }

    private void handleCommit() {
        long lastCommittedMessageId = replicationProtocol.publish(null);
        if (lastCommittedMessageId != 0)
            removeCommittedEntries(lastCommittedMessageId);
    }

    private void removeCommittedEntries(long lastCommittedMessageId) {
        while (!queue.isEmpty()) {
            EntryInfo info = queue.peek();

            if (info.entry.getMessageId() <= lastCommittedMessageId) {
                queue.poll();

                info.commitHandler.onSucceeded(info.entry.getValue());

                checkFlow();
            } else {
                break;
            }
        }
    }

    private void clear() {
        nextMessageId = 1;

        SystemException exception = new SystemException(messages.serverNotLeader());
        while (!queue.isEmpty()) {
            EntryInfo info = queue.poll();
            info.commitHandler.onFailed(exception);
        }

        checkFlow();
    }

    private void checkFlow() {
        if (flowController == null)
            return;

        if (!flowLocked && queue.size() >= lockQueueCapacity) {
            flowLocked = true;
            flowController.lockFlow(FlowType.PUBLISH);
        }

        if (flowLocked && queue.size() <= unlockQueueCapacity) {
            flowLocked = false;
            flowController.unlockFlow(FlowType.PUBLISH);
        }
    }

    private  static class EntryInfo {
        private final LogEntry entry;
        private final ICompletionHandler<ByteArray> commitHandler;

        private EntryInfo(LogEntry entry, ICompletionHandler<ByteArray> commitHandler) {
            this.entry = entry;
            this.commitHandler = commitHandler;
        }
    }

    private interface IMessages {
        @DefaultMessage("Current server is not a leader.")
        ILocalizedMessage serverNotLeader();
    }
}
