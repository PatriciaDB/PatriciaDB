package io.patriciadb.fs;

import io.patriciadb.utils.lifecycle.PatriciaController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TaskScheduledExecutor implements PatriciaController {
    private final static Logger log = LoggerFactory.getLogger(TaskScheduledExecutor.class);
    private final ScheduledExecutorService executorService;

    public TaskScheduledExecutor(ScheduledExecutorService executorService) {
        this.executorService = executorService;
    }

    public ScheduledExecutorService getExecutorService() {
        return executorService;
    }

    @Override
    public void destroy() throws Exception {
        log.info("Destroying TaskScheduledExecutor");
        executorService.shutdown();
        log.info("Awaiting termination");

        executorService.awaitTermination(10, TimeUnit.SECONDS);
        executorService.shutdownNow();
        if(executorService.isTerminated()) {
            log.info("TaskScheduledExecutor terminated");
        } else {
            log.warn("TaskScheduledExecutor not terminated after timeout");
        }
    }
}
