package com.artos.tests.core.mocks;

import com.artos.impl.core.server.utils.Utils;
import com.artos.spi.core.FlowType;
import com.artos.spi.core.IStateMachine;
import com.artos.spi.core.IStateMachineTransaction;
import com.exametrika.common.io.impl.ByteInputStream;
import com.exametrika.common.io.impl.ByteOutputStream;
import com.exametrika.common.io.impl.DataDeserialization;
import com.exametrika.common.io.impl.DataSerialization;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.Exceptions;
import com.exametrika.common.utils.IOs;

import java.io.InputStream;
import java.io.OutputStream;

public class StateMachineMock implements IStateMachine {
    private StateMachineTransactionMock transaction = new StateMachineTransactionMock();
    private ByteArray data;
    private Throwable error;

    public StateMachineTransactionMock getTransaction() {
        return transaction;
    }

    public ByteArray getData() {
        return data;
    }

    public void setData(ByteArray data) {
        this.data = data;
    }

    public void setError(Throwable error) {
        this.error = error;
    }

    @Override
    public IStateMachineTransaction beginTransaction(boolean readOnly) {
        transaction.init(readOnly);
        return transaction;
    }

    @Override
    public long acquireSnapshot(OutputStream stream) {
        try {
            if (error != null)
                throw error;

            ByteOutputStream out = new ByteOutputStream();
            DataSerialization serialization = new DataSerialization(out);
            serialization.writeByteArray(data);
            Utils.writeGroupConfiguration(serialization, transaction.getConfiguration());

            ByteInputStream in = new ByteInputStream(out.getBuffer(), 0, out.getLength());

            IOs.copy(in, stream);
            return transaction.getLastCommitIndex();
        } catch (Throwable e) {
           return Exceptions.wrapAndThrow(e);
        }
    }

    @Override
    public void applySnapshot(long logIndex, InputStream stream) {
        try {
            if (error != null)
                throw error;

            ByteOutputStream out = new ByteOutputStream();

            IOs.copy(stream, out);

            ByteInputStream in = new ByteInputStream(out.getBuffer(), 0, out.getLength());
            DataDeserialization deserialization = new DataDeserialization(in);
            data = deserialization.readByteArray();
            transaction.setConfiguration(Utils.readGroupConfiguration(deserialization));
            transaction.setLastCommitIndex(logIndex);

        } catch (Throwable e) {
            Exceptions.wrapAndThrow(e);
        }
    }

    @Override
    public void lockFlow(FlowType flow) {
    }

    @Override
    public void unlockFlow(FlowType flow) {
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
}
