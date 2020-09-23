package com.aiven.test.commons;

//

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: sbenner
 * Date: 7/5/17
 * Time: 8:13 PM
 */
public class ContextProvider {


    public static Properties contextProperties = null;
    private static ExecutorService executor =
            new ThreadPoolExecutor(5, 10,
                    2, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>());

    public static void init() throws IOException {
        contextProperties = loadProperties();
    }

    public static Properties loadProperties() throws IOException {
        Properties properties =
                new Properties();
        properties.load(ContextProvider.class.getClassLoader().getResourceAsStream("app.properties"));
        return properties;
    }


    public static ExecutorService getExecutor() {
        if (executor.isShutdown()) {
            executor = new ThreadPoolExecutor(5, 10,
                    5, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>());
        }
        return executor;
    }

}
