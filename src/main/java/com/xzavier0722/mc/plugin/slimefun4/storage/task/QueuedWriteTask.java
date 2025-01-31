package com.xzavier0722.mc.plugin.slimefun4.storage.task;

import com.xzavier0722.mc.plugin.slimefun4.storage.common.RecordKey;
import io.github.thebusybiscuit.slimefun4.core.debug.Debug;
import io.github.thebusybiscuit.slimefun4.implementation.Slimefun;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Level;

public class QueuedWriteTask implements Runnable {
    private final Queue<RecordKey> queue = new LinkedList<>();
    private final Map<RecordKey, Runnable> tasks = new HashMap<>();
    private volatile boolean done = false;
    private volatile boolean aborted = false;

    @Override
    public final void run() {
        if (aborted) {
            Slimefun.logger().log(Level.INFO,"Write Task Aborted");
            return;
        }

        var task = next();
        while (!aborted && task != null) {
            try {
                task.run();
            } catch (Throwable e) {
                onError(e);
            }
            task = next();
        }

        try {
            onSuccess();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    protected void onSuccess() {}

    protected void onError(Throwable e) {}

    public synchronized boolean queue(RecordKey key, Runnable next) {
        if (done || aborted) {
            return false;
        }

        if (tasks.put(key, next) == null) {
            return queue.offer(key);
        }
        return true;
    }

    public void abort() {
        aborted = true;
    }

    private synchronized Runnable next() {
        var key = queue.poll();
        if (key == null) {
            done = true;
            return null;
        }
        return tasks.remove(key);
    }
}
