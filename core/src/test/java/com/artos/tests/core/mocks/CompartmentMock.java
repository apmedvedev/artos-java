package com.artos.tests.core.mocks;

import com.exametrika.common.compartment.ICompartment;
import com.exametrika.common.compartment.ICompartmentGroup;
import com.exametrika.common.compartment.ICompartmentProcessor;
import com.exametrika.common.compartment.ICompartmentTask;
import com.exametrika.common.compartment.ICompartmentTimerProcessor;

import java.util.List;

public class CompartmentMock implements ICompartment {
    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean isStarted() {
        return true;
    }

    @Override
    public ICompartmentGroup getGroup() {
        return null;
    }

    @Override
    public long getDispatchPeriod() {
        return 0;
    }

    @Override
    public void setDispatchPeriod(long period) {

    }

    @Override
    public int getMinLockQueueCapacity() {
        return 0;
    }

    @Override
    public void setMinLockQueueCapacity(int value) {

    }

    @Override
    public int getMaxUnlockQueueCapacity() {
        return 0;
    }

    @Override
    public void setMaxUnlockQueueCapacity(int value) {

    }

    @Override
    public int getTaskBatchSize() {
        return 0;
    }

    @Override
    public void setTaskBatchSize(int value) {

    }

    @Override
    public void addTimerProcessor(ICompartmentTimerProcessor processor) {

    }

    @Override
    public void removeTimerProcessor(ICompartmentTimerProcessor processor) {

    }

    @Override
    public void addProcessor(ICompartmentProcessor processor) {

    }

    @Override
    public void removeProcessor(ICompartmentProcessor processor) {

    }

    @Override
    public void offer(ICompartmentTask task) {
    }

    @Override
    public void offer(Runnable task) {
        task.run();
    }

    @Override
    public void offer(List<?> tasks) {

    }

    @Override
    public boolean execute(ICompartmentTask task) {
        return false;
    }

    @Override
    public boolean execute(Runnable task) {
        return false;
    }

    @Override
    public void wakeup() {

    }

    @Override
    public void lockFlow(Object flow) {

    }

    @Override
    public void unlockFlow(Object flow) {

    }

    @Override
    public long getCurrentTime() {
        return 0;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
