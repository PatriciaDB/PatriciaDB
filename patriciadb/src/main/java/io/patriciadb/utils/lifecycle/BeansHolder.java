package io.patriciadb.utils.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

public class BeansHolder {
    private final static Logger log = LoggerFactory.getLogger(BeansHolder.class);

    private final ArrayList<ControllerHolder> beans = new ArrayList<>();
    private  AppState appState = AppState.INITIALISING;

    private enum AppState {INITIALISING, STARTED, DESTROYED}


    public synchronized <T extends PatriciaController> T addBean(ControllerSupplier<T> beanSupplier) throws Exception {
        if(appState != AppState.INITIALISING) {
            throw new IllegalStateException("Invalid state");
        }
        T bean = beanSupplier.get();
        beans.add(new ControllerHolder(bean));
        return bean;
    }


    public synchronized void start() throws Exception {
        if(appState != AppState.INITIALISING) {
            throw new IllegalStateException("Invalid state");
        }
        try {
            for (var bean : beans) {
                bean.controller.initialize();
                bean.state.set(ControllerState.INITIALISED);
            }
            appState = AppState.STARTED;
        } catch (Throwable t) {
            log.error("Error while initialising library", t);

            for (var bean : beans) {
                if(bean.state.get()==ControllerState.INITIALISED) {
                    try {
                        bean.controller.destroy();
                    } catch (Throwable t2) {
                        //nope
                    }
                }
            }
            appState = AppState.DESTROYED;
            throw t;
        }
    }

    public synchronized void shutdown() throws Exception {
        if(appState != AppState.STARTED) {
            throw new IllegalStateException("Invalid state");
        }
        log.info("Shutting down");
        for (int i = beans.size() - 1; i >= 0; i--) {
            try {
                beans.get(i).controller.preDestroy();
            } catch (Throwable e) {
                log.error("Bean failed on preDestroy", e);
            }
        }
        for (int i = beans.size() - 1; i >= 0; i--) {
            try {
                beans.get(i).controller.destroy();
                beans.get(i).state.set(ControllerState.DESTROYED);
            } catch (Throwable e) {
                log.error("Bean failed on destroy", e);
            }
        }
        appState = AppState.DESTROYED;
    }


   private enum ControllerState {CREATED, INITIALISED, DESTROYED}

    private static class ControllerHolder {
        private final PatriciaController controller;
        private final AtomicReference<ControllerState> state = new AtomicReference<>(ControllerState.CREATED);

        public ControllerHolder(PatriciaController controller) {
            this.controller = controller;
        }
    }


    public interface ControllerSupplier<T extends PatriciaController> {
        T get() throws Exception;
    }

}
