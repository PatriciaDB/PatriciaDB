package io.patriciadb.utils.finalizer;

import io.patriciadb.fs.disk.transaction.TransactionManager;
import io.patriciadb.utils.lifecycle.PatriciaController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Finalizer implements PatriciaController {

    private final static Logger log = LoggerFactory.getLogger(Finalizer.class);

    private final ReferenceQueue<Object> referenceQueue = new ReferenceQueue<>();
    private final FinalizerThread transactionFinalizerThread = new FinalizerThread();
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final Set<CleanerInternal> cleanerInternalSet = Collections.synchronizedSet(new HashSet<>());


    @Override
    public void initialize() throws Exception {
        transactionFinalizerThread.setDaemon(true);
        transactionFinalizerThread.setName("PatriciaFinalizer");
        transactionFinalizerThread.start();
    }

    public void register(Object ref, Runnable runnable) {
        Objects.requireNonNull(runnable);
        Objects.requireNonNull(ref);
        cleanerInternalSet.add(new CleanerInternal(ref, referenceQueue, runnable));

    }

    private static class CleanerInternal extends PhantomReference<Object> {

        private final Runnable callback;

        public CleanerInternal(Object referent, ReferenceQueue<? super Object> q, Runnable callback) {
            super(referent, q);
            this.callback = callback;
        }

        public void callCleanUp() {
            callback.run();
        }
    }


    @Override
    public void destroy() {
        isRunning.set(false);
        transactionFinalizerThread.interrupt();
    }

    private class FinalizerThread extends Thread {
        public void run() {

            while (isRunning.get()) {
                try {
                    var cleaner = (CleanerInternal) referenceQueue.remove();
                    cleaner.callCleanUp();
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                } catch (Throwable t) {
                    log.error("unexpected error", t);
                }
            }
        }
    }


}
