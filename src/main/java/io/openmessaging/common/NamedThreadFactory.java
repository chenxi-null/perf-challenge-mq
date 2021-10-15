package io.openmessaging.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chenxi20
 * @date 2021/10/15
 */
public class NamedThreadFactory implements ThreadFactory {

    private static final Logger log = LoggerFactory.getLogger(NamedThreadFactory.class);

    private final AtomicInteger num = new AtomicInteger();

    private final ThreadGroup group;

    private final String name;

    private final Thread.UncaughtExceptionHandler exceptionHandler = (t, e) -> log.error("[{}] thread occur exception", t.getName(), e);

    public NamedThreadFactory(String threadName) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();

        this.name = threadName;
    }

    @Override
    public Thread newThread(Runnable r) {
        int n = num.getAndIncrement();
        String tName = name + (n == 0 ? "" : "-" + n);
        Thread t = new Thread(group, r, name);
        t.setName(tName);
        t.setUncaughtExceptionHandler(this.exceptionHandler);
        return t;
    }
}
