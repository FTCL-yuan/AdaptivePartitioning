package app;

import com.codahale.metrics.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import partitioner.*;
import partitioner.cardinalityAwarePartitioners.AMPartitioner;
import partitioner.cardinalityAwarePartitioners.CMPartitioner;
import partitioner.cardinalityAwarePartitioners.cAMPartitioner;
import util.Config;
import java.io.File;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WordCount {
    public static void main(String[] args) throws Exception {
        /**
         * Kafka configuration
         */
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "192.168.226.129:9092");
        kafkaProperties.put("zookeeper.connect", "192.168.226.33:2181");
        kafkaProperties.put("group.id", "metric-group");
        kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProperties.put("auto.offset.reset", Config.KAFKA_OFFSET);

        /**
         * Metric configuration
         */
        final String metricPath = Config.METRICS_PATH;

        /**
         * App configuration
         */
        final String kafkaTopic = Config.KAFKA_TOPIC;
        final String partitioningMethod = Config.PARTITIONING_METHOD;
        final Integer sourceParallelism = Config.SOURCE_PARALLELISM;
        final Integer mapParallelism = Config.MAP_PARALLELISM;
        final Integer preReduceParallelism = Config.PRE_REDUCE_PARALLELISM;
        final Integer reduceParallelism = Config.REDUCE_PARALLELISM;
        final Long preReduceProcessTime = Config.PRE_REDUCE_PROCESS_TIME;
        final Long reduceProcessTime = Config.REDUCE_PROCESS_TIME;
        final Long reduceInterval = Config.REDUCE_INTERVAL;
        final Long cacheSize = Config.CACHE_SIZE;
        final Long cacheInterval = Config.CACHE_INTERVAL;
        final Long bufferTimeout = Config.BUFFER_TIMEOUT;
        final Integer choiceNumber = Config.CHOICE_NUMBER;


        final Logger logger = LoggerFactory.getLogger(WordCount.class);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(bufferTimeout);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        DataStream<String> source = env.addSource(new FlinkKafkaConsumer010<String>(kafkaTopic,new SimpleStringSchema(),kafkaProperties)).setParallelism(sourceParallelism);
        DataStream<Tuple1<String>> wordInitial = source.flatMap(new FlatMapFunction<String, Tuple1<String>>() {

            @Override
            public void flatMap(String s, Collector<Tuple1<String>> collector) {
                String words[] = s.split(" ");
                for (int i = 0;i < words.length;i++){
                    String word = words[i].trim().toLowerCase();
                    if (word != null && !word.isEmpty()){
                        collector.collect(new Tuple1<>(word));
                    }
                }
            }
        }).setParallelism(mapParallelism);
        DataStream<Tuple1<String>> wordPartition;
        System.out.println(partitioningMethod);
        switch (partitioningMethod){
            case "KG":
                wordPartition = wordInitial.partitionCustom(new HashPartitioner<>(),0);
                break;
            case "SG":
                wordPartition = wordInitial.partitionCustom(new ShufflePartitionerRoundRobin<>(),0);
                break;
            case "FW":
                wordPartition = wordInitial.forward();
                break;
            case "GP":
                wordPartition = wordInitial.global();
                break;
            case "PKG":
                wordPartition = wordInitial.partitionCustom(new PKGPartitioner(),0);
                break;
            case "DC":
                wordPartition = wordInitial.partitionCustom(new DChoicePartitioner(),0);
                break;
            case  "WC":
                wordPartition = wordInitial.partitionCustom(new WChoicePartitioner(),0);
                break;
            case "DA":
                wordPartition = wordInitial.partitionCustom(new DistributionAwarePartitioner<>(),0);
                break;
            case "AP":
                wordPartition = wordInitial.partitionCustom(new AdaptivePartitioner<>(),0);
                break;
            case "AM":
                wordPartition = wordInitial.partitionCustom(new AMPartitioner<>(choiceNumber),0);
                break;
            case "CM":
                wordPartition = wordInitial.partitionCustom(new CMPartitioner<>(choiceNumber),0);
                break;
            case "cAM":
                wordPartition = wordInitial.partitionCustom(new cAMPartitioner<>(choiceNumber),0);
                break;
                default:
                    wordPartition = wordInitial.keyBy(0);
        }
        DataStream<Tuple5<String, Long, Long, Long, Long>> preReduce = wordPartition.process(new ProcessFunction<Tuple1<String>, Tuple5<String, Long, Long, Long, Long>>() {
            int taskIndex;
            int parallelism;
            String hostname;
            HashMap<String, Long> countMap;
            long processTime,collectInterval,maxCacheSize,maxCacheInterval,windowStart,windowEnd,cacheEnd;
            boolean windowEndFlag, cacheFlushFlag;
//            HashSet<String> cacheSet;

            private transient Histogram preReduceLatencyHistogram,networkAndQueueLatencyHistogram,preReduceMemorySizeHistogram;
            private transient Meter throughputMeter;
            private transient MetricRegistry registry;
            private transient CsvReporter reporter;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                String taskNameWithSubtask = getRuntimeContext().getTaskNameWithSubtasks();
                System.out.println(taskNameWithSubtask);
                taskIndex = getRuntimeContext().getIndexOfThisSubtask();
                parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
                hostname = InetAddress.getLocalHost().getHostName();
                processTime = preReduceProcessTime;
                collectInterval = reduceInterval;
                maxCacheSize = cacheSize;
                maxCacheInterval = cacheInterval;
                windowEnd = 0;
                cacheEnd = 0;
                countMap = new HashMap<>();
                registry = new MetricRegistry();
                reporter = CsvReporter.forRegistry(registry)
                        .formatFor(Locale.US)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .build(new File(metricPath));
                reporter.start(1,TimeUnit.SECONDS);
                preReduceLatencyHistogram = new Histogram(new SlidingTimeWindowReservoir(10,TimeUnit.SECONDS));
                registry.register("preReduce_latency"+taskIndex+"@"+hostname,preReduceLatencyHistogram);
                networkAndQueueLatencyHistogram = new Histogram(new SlidingTimeWindowReservoir(10,TimeUnit.SECONDS));
                registry.register("networkAndQueue_latency"+taskIndex+"@"+hostname,networkAndQueueLatencyHistogram);
                preReduceMemorySizeHistogram = new Histogram(new SlidingTimeWindowReservoir(10,TimeUnit.SECONDS));
                registry.register("preReduce_memorySize"+taskIndex+"@"+hostname,preReduceMemorySizeHistogram);
                throughputMeter = new Meter();
                registry.register("preReduce_throughput"+taskIndex+"@"+hostname,throughputMeter);
            }

            @Override
            public void processElement(Tuple1<String> tuple1, Context context, Collector<Tuple5<String, Long, Long, Long, Long>> collector) throws Exception {
                long preReduceStartTime = System.currentTimeMillis();
                long ingestionTime = context.timestamp();
                testWait(processTime);

                if (windowEnd == 0){
                    windowStart = ingestionTime - ingestionTime % collectInterval;
                    windowEnd = windowStart + collectInterval;
                    windowEndFlag = false;
                }
                if (ingestionTime >= windowEnd){
                    windowEndFlag = true;
                }
                if (cacheEnd == 0){
                    cacheEnd = ingestionTime - ingestionTime % maxCacheInterval + maxCacheInterval;
                }
                if (ingestionTime >= cacheEnd){
                    cacheFlushFlag = true;
                }
                if (windowEndFlag || cacheFlushFlag || countMap.size() >= maxCacheSize){
                    for (Map.Entry<String,Long> entry : countMap.entrySet()){
                        String word = entry.getKey();
                        Long count = entry.getValue();
                        Long preReduceEndTime = System.currentTimeMillis();
                        collector.collect(new Tuple5<>(word, count, ingestionTime, preReduceEndTime, windowEnd - 1));
                    }
                    countMap.clear();
                }
                if (windowEndFlag){
                    windowStart = ingestionTime - ingestionTime % collectInterval;
                    windowEnd = windowStart + collectInterval;
                    windowEndFlag = false;
                }
                if (cacheFlushFlag){
                    cacheEnd = ingestionTime - ingestionTime % maxCacheInterval + maxCacheInterval;
                    cacheFlushFlag = false;
                }
                if (ingestionTime >= windowStart){
                    String word = tuple1.getField(0);
                    if(!word.isEmpty()){
                        Long count = countMap.get(word);
                        if (count == null){
                            count = 0L;
                        }
                        count++;
                        countMap.put(word, count);
                    }
                }
                long currentTime = System.currentTimeMillis();
                this.preReduceMemorySizeHistogram.update(countMap.size());
                long networkAndQueueLatency = preReduceStartTime - ingestionTime;
                long preReduceLatency = currentTime-ingestionTime;
                networkAndQueueLatencyHistogram.update(networkAndQueueLatency);
                preReduceLatencyHistogram.update(preReduceLatency);
                throughputMeter.mark();
            }

            @Override
            public void close() throws Exception {
                reporter.close();
                logger.info("TaskIndex::"+taskIndex);
                logger.info("CountMap::"+countMap);
                logger.info("CountMapSize:"+countMap.size());
                super.close();
            }
        }).setParallelism(preReduceParallelism).name("preReduce");
        DataStream<Tuple2<String,Long>> reduce = preReduce.keyBy(0).process(new KeyedProcessFunction<Tuple, Tuple5<String, Long, Long, Long, Long>, Tuple2<String, Long>>() {
            HashMap<String, Long> totalCountMap;
            long processTime,collectInterval,windowStart,windowEnd;
            boolean windowEndFlag;

            int taskIndex;
            int parallelism;
            String hostname;

            private transient Histogram reduceLatencyHistogram,reduceNetworkAndQueueLatencyHistogram,reduceMemorySizeHistogram;
            private transient Meter throughputMeter;
            private transient MetricRegistry registry;
            private transient CsvReporter reporter;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                collectInterval = reduceInterval;
                windowStart = windowEnd = 0;
                totalCountMap = new HashMap<>();
                processTime  = reduceProcessTime;
                taskIndex = getRuntimeContext().getIndexOfThisSubtask();
                parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
                hostname = InetAddress.getLocalHost().getHostName();
                registry = new MetricRegistry();
                reporter = CsvReporter.forRegistry(registry)
                        .formatFor(Locale.US)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .build(new File(metricPath));
                reporter.start(1,TimeUnit.SECONDS);

                reduceLatencyHistogram = new Histogram(new SlidingTimeWindowReservoir(10,TimeUnit.SECONDS));
                registry.register("reduce_latency"+taskIndex+"@"+hostname,reduceLatencyHistogram);
                reduceNetworkAndQueueLatencyHistogram = new Histogram(new SlidingTimeWindowReservoir(10,TimeUnit.SECONDS));
                registry.register("reduce_networkAndQueue_latency"+taskIndex+"@"+hostname,reduceNetworkAndQueueLatencyHistogram);
                reduceMemorySizeHistogram = new Histogram(new SlidingTimeWindowReservoir(10,TimeUnit.SECONDS));
                registry.register("reduce_memorySize"+taskIndex+"@"+hostname,reduceLatencyHistogram);
                throughputMeter = new Meter();
                registry.register("reduce_throughput"+taskIndex+"@"+hostname,throughputMeter);
            }

            @Override
            public void processElement(Tuple5<String, Long, Long, Long, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                long reduceStartTime = System.currentTimeMillis();
                long ingestionTime = value.getField(2);
                long preReduceEndTime = value.getField(3);
                long windowTime = value.getField(4);
                testWait(processTime);
                if (windowEnd == 0){
                    windowStart = windowTime - windowTime % collectInterval;
                    windowEnd = windowStart + collectInterval;
                    windowEndFlag = false;
                }
                if (windowTime >= windowEnd){
                    windowEndFlag = true;
                }
                if (windowEndFlag){
                    for (Map.Entry<String,Long> entry : totalCountMap.entrySet()){
                        String word = entry.getKey();
                        Long count = entry.getValue();
                        out.collect(new Tuple2<>(word, count));
                    }
                    windowStart += collectInterval;
                    windowEnd += collectInterval;
                    windowEndFlag = false;
                    totalCountMap.clear();
                }
               if (windowTime >= windowStart){
                    String word = value.getField(0);
                    Long count = value.getField(1);
                   if(!word.isEmpty()){
                       Long totalCount = totalCountMap.get(word);
                       if (totalCount == null){
                           totalCount = 0L;
                       }
                       totalCount = totalCount + count;
                       totalCountMap.put(word, totalCount);
                   }
                }
                long currentTime = System.currentTimeMillis();
                long reduceLatency = currentTime - preReduceEndTime;
                long networkAndQueueLatency = reduceStartTime - preReduceEndTime;
                reduceLatencyHistogram.update(reduceLatency);
                reduceNetworkAndQueueLatencyHistogram.update(networkAndQueueLatency);
                reduceMemorySizeHistogram.update(totalCountMap.size());
                throughputMeter.mark();
            }
            @Override
            public void close() throws Exception {
                reporter.close();
                logger.info("TaskIndex:"+taskIndex);
                logger.info("CountMap:"+totalCountMap);
                logger.info("CountMapSize:"+totalCountMap.size());
                super.close();
            }
        }).setParallelism(reduceParallelism).name("reduce").slotSharingGroup("reduce");
        env.execute();
    }
    public static void testWait(long INTERVAL){
        long start = System.nanoTime();
        long end;
        do{
            end = System.nanoTime();
        }while(start + INTERVAL >= end);
    }
}
