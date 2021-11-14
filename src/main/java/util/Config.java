package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Config {
    public static String METRICS_PATH;
    public static String KAFKA_TOPIC;
    public static String KAFKA_OFFSET;
    //ns
    public static long PRE_REDUCE_PROCESS_TIME;
    public static long REDUCE_PROCESS_TIME;
    //ms
    public static long REDUCE_INTERVAL;
    public static long CACHE_INTERVAL;
    public static long CACHE_SIZE;
    public static long BUFFER_TIMEOUT;

    public static Integer SOURCE_PARALLELISM;
    public static Integer MAP_PARALLELISM;
    public static Integer PRE_REDUCE_PARALLELISM;
    public static Integer REDUCE_PARALLELISM;
    public static String PARTITIONING_METHOD;
    public static Integer CHOICE_NUMBER;
    static {
        String appProperties;
        if (System.getProperty("os.name").contains("Win"))
            appProperties = "D:\\flink\\flink\\properties\\wordcount.properties";
        else
            appProperties= "/root/flink/properties/wordcount.properties";
        File file = new File(appProperties);
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(file));
            METRICS_PATH = properties.getProperty("metrics.path");
            KAFKA_TOPIC = properties.getProperty("kafka.topic");
            KAFKA_OFFSET = properties.getProperty("kafka.offset");
            PRE_REDUCE_PROCESS_TIME = Long.parseLong(properties.getProperty("pre-reduce.process.time"));
            REDUCE_PROCESS_TIME = Long.parseLong(properties.getProperty("reduce.process.time"));
            REDUCE_INTERVAL = Long.parseLong(properties.getProperty("reduce.interval"));
            CACHE_INTERVAL = Long.parseLong(properties.getProperty("cache.interval"));
            CACHE_SIZE = Long.parseLong(properties.getProperty("cache.size"));
            BUFFER_TIMEOUT = Long.parseLong(properties.getProperty("buffer.timeout"));

            SOURCE_PARALLELISM = Integer.parseInt(properties.getProperty("source.parallelism"));
            MAP_PARALLELISM = Integer.parseInt(properties.getProperty("map.parallelism"));
            PRE_REDUCE_PARALLELISM = Integer.parseInt(properties.getProperty("pre-reduce.parallelism"));
            REDUCE_PARALLELISM = Integer.parseInt(properties.getProperty("reduce.parallelism"));
            PARTITIONING_METHOD = properties.getProperty("partitioning.method");
            CHOICE_NUMBER = Integer.parseInt(properties.getProperty("choice.number"));
        } catch (IOException e) {
            if (System.getProperty("os.name").contains("Win"))
                METRICS_PATH = "D:\\flink\\flink\\metric";
            else
                METRICS_PATH = "/opt/flink-1.7.1/metric";
            KAFKA_TOPIC = "zipWord";
            KAFKA_OFFSET="latest";
            PRE_REDUCE_PROCESS_TIME = 1000000L;
            REDUCE_PROCESS_TIME = 1000000L;
            REDUCE_INTERVAL = 10000;
            CACHE_INTERVAL = 10;
            CACHE_SIZE = 50;
            BUFFER_TIMEOUT = 1;
            MAP_PARALLELISM = 1;
            SOURCE_PARALLELISM = 1;
            PRE_REDUCE_PARALLELISM = 1;
            REDUCE_PARALLELISM = 1;
            PARTITIONING_METHOD = "AP";
            CHOICE_NUMBER = 5;
            e.printStackTrace();
        }
    }
}
