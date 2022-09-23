package io.patriciadb.fs.disk;

import java.util.concurrent.ThreadFactory;

public class FsThreadFactory implements ThreadFactory {

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(false);
        thread.setName("PatriciaFileSystem");
        thread.setPriority(Thread.NORM_PRIORITY);
        return thread;
    }
}
