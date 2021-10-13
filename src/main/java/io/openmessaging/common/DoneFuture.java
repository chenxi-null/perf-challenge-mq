package io.openmessaging.common;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * @author chenxi <chenxi01@souche.com>
 * @date 2020/2/19
 */
public class DoneFuture<T> implements Future<T> {

    private T result;

    private final Sync sync = new Sync();

    @Override
    public boolean isDone() {
        return sync.isDone();
    }

    public void done(T result) {
        this.result = result;
        sync.release(1);
    }

    @Override
    public T get() {
        sync.acquire(-1);
        return result;
    }

    @Override
    public T get(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }


    // https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/AbstractQueuedSynchronizer.html
    static class Sync extends AbstractQueuedSynchronizer {

        private static final long serialVersionUID = 33187619902973584L;

        // future status
        private static final int DONE = 1;
        private static final int PENDING = 0;

        @Override
        protected boolean tryAcquire(int acquires) {
            return getState() == DONE;
        }

        @Override
        protected boolean tryRelease(int releases) {
            if (getState() == PENDING) {
                return compareAndSetState(PENDING, DONE);
            }
            return false;
        }

        public boolean isDone() {
            return getState() == DONE;
        }
    }
}
