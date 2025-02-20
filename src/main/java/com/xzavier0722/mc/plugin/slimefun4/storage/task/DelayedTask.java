package com.xzavier0722.mc.plugin.slimefun4.storage.task;

import lombok.Getter;

import java.util.concurrent.TimeUnit;

public class DelayedTask {
    private final Runnable task;
    private long runAfter = 0;

    @Getter
    private volatile boolean executed = false;

    public DelayedTask(long delay, TimeUnit unit, Runnable task) {
        this.task = task;
        setRunAfter(delay, unit);
    }

    public synchronized void setRunAfter(long delay, TimeUnit unit) {
        runAfter = System.currentTimeMillis() + unit.toMillis(delay);
    }

    public synchronized boolean tryRun() {
        if (System.currentTimeMillis() < runAfter) {
            return false;
        }

        executed = true;
        task.run();
        return true;
    }

    public void runUnsafely() {
        executed = true;
        task.run();
    }
}
