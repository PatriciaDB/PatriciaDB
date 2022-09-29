package io.patriciadb.fs;

import io.patriciadb.utils.lifecycle.PatriciaController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CallbackExecutor implements PatriciaController {
    private final static Logger log = LoggerFactory.getLogger(CallbackExecutor.class);
    private final ThreadPoolExecutor executor;

    public CallbackExecutor(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

    public ThreadPoolExecutor getExecutor() {
        return executor;
    }

    @Override
    public void destroy() throws Exception {
        log.info("CallbackExecutor shutting down");
        executor.shutdown();
        log.info("Awaiting termination");
        executor.awaitTermination(10, TimeUnit.SECONDS);
        executor.shutdownNow();
        if(executor.isTerminated()) {
            log.info("TaskScheduledExecutor terminated");
        } else {
            log.warn("TaskScheduledExecutor not terminated after timeout");
        }
    }
}
