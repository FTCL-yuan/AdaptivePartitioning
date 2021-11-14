package partitioner;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;
import util.KeyTransformation;
import util.TimeSlidingWindowStreamSummary;

import java.util.*;

public class DistributionAwarePartitioner<K> implements Partitioner<K> {
    private final HashFunction hashFunction = Hashing.murmur3_128(17);
    private long[] workerLoads;
    private Map<K, Set<Integer>> routingTable = new HashMap<>();
    private StreamSummary<K> spaceSaving;
    TimeSlidingWindowStreamSummary<K> timeSlidingWindowStreamSummary;
    private float delta;
    private final double alpha = 0.5;
    private long totalCount = 0;
    private int instanceNum = 0;
    private long usedSize;
    private int windowNum;
    private int capacity;
    private Long windowSize;
    private double epsilonFactor;
    private int coDomain;
    @Override
    public int partition(K key, int numPartitions) {
        int target = 0;
        /*
        if(spaceSaving==null){
            workerLoads = new long[numPartitions];
            int capacity = numPartitions*100;
            delta = 1.0/(capacity);
            spaceSaving = new StreamSummary<>(capacity);

        }
        */

        if (instanceNum != numPartitions){
            instanceNum = numPartitions;
            workerLoads = new long[numPartitions];
//            serversNo = currentTargetTaskNum;
            epsilonFactor = 32.0;
            usedSize = (long) (10 * instanceNum*epsilonFactor);

            //usedSize = 10000L;
            windowNum = 3;
            capacity = (int) (instanceNum * 100);
            delta = 1.0f/(instanceNum*100);
            windowSize = usedSize * (windowNum - 1);
            coDomain = (int) Math.ceil(epsilonFactor*instanceNum);
            timeSlidingWindowStreamSummary = new TimeSlidingWindowStreamSummary<>(windowNum, windowSize, capacity, coDomain);
        }

        totalCount++;
        //spaceSaving.offer(key);
        int targetTaskId;
        byte[] raw = KeyTransformation.hashCodeGet(key);
        int tmpHashCode =FastMath.abs( hashFunction.hashBytes(raw).asInt() % coDomain);
        //hll.addRaw(hashFunction.hashBytes(raw).asLong());

        timeSlidingWindowStreamSummary.offer(key,tmpHashCode);

        //double fk = EstimateFrequency(key);
        boolean frequentFlag = isFrequent(key,delta);

        if(!frequentFlag){
            //target = (int)(Math.abs(hashFunction.hashBytes(key.toString().getBytes()).asLong())%numPartitions);
           target =  FastMath.abs(hashFunction.hashBytes(raw).asInt() % instanceNum);
        }else{
            double[] score = calScore(key, numPartitions);

            double maxScore = Double.MIN_VALUE;
            for(int i=0;i<numPartitions;i++){
                if(score[i]>maxScore){
                    maxScore = score[i];
                    target = i;
                }
            }
            Set<Integer> candidates = routingTable.getOrDefault(key,new HashSet<>());
            candidates.add(target);
            routingTable.put(key,candidates);
        }
        workerLoads[target]++;
        return target;
    }

    private double[] calScore(K key, int numPartitions) {
        double[] scores = new double[numPartitions];
        for(int i=0;i<numPartitions;i++){
            Set<Integer> candidate = routingTable.get(key);
            int l;
            if(candidate==null || !candidate.contains(i)){
                l = 0;
            }else{
                l = 1;
            }
            double imbalance = calImbalance(i,numPartitions);
            scores[i] = (1-alpha)*l-alpha*imbalance;
        }
        return scores;
    }

    private double calImbalance(int i, int numPartitions) {
        long nowLoad = workerLoads[i];
        int increase = 1;
        double avgLoad = totalCount/numPartitions;
        double imbalance = ((nowLoad+increase)-avgLoad)/avgLoad;
        return imbalance;
    }

    private double EstimateFrequency(K key) {
        double freq = 0;
        List<Counter<K>> topk = spaceSaving.topK(spaceSaving.size());
        for(Counter<K> counter:topk){
            K item = counter.getItem();
            long count = counter.getCount();
            if(item.equals(key)){
                freq = 1.0*count/totalCount;
                break;
            }
        }
        return freq;
        //return 0;
    }
    private boolean isFrequent(K key,float probability){
        HashMap<K,Long> highFreqList = timeSlidingWindowStreamSummary.getTopK(probability);
        if (highFreqList.containsKey(key)){
            return true;
        }else
            return false;
    }
}
