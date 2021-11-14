package partitioner.cardinalityAwarePartitioners;

import net.agkn.hll.HLL;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;
import util.KeyTransformation;
import util.Seed;

public class CMPartitioner<K> implements Partitioner<K> {
    private int choiceNumber;
    private  double[] targetTaskStats;
    private HashFunction[] hash;
    private Seed seeds;
    transient HLL[] hlls;
    int instanceNum = 0;


    long countWindow,timeWindow,clearCount,cleanTime;
    public CMPartitioner(int choiceNumber){
        this.choiceNumber = choiceNumber;
    }
    public CMPartitioner(){
        this.choiceNumber = 2;
    }
    @Override
    public int partition(K key, int numPartitions) {
        if (instanceNum != numPartitions){
            instanceNum = numPartitions;
            seeds = new Seed(instanceNum);
            hash = new HashFunction[instanceNum];
            for (int i = 0; i < hash.length; i++) {
                hash[i] = Hashing.murmur3_128(seeds.SEEDS[i]);
            }
            targetTaskStats = new double[instanceNum];
            hlls = new HLL[instanceNum];
            for (int i = 0; i < instanceNum;i++){
                hlls[i] = new HLL(12,5);
            }

//            countWindow = 10000;
//            clearCount = 0;
//
            timeWindow = 240000;
            cleanTime = System.currentTimeMillis() + timeWindow;

        }

//        clearCount++;
//        if (clearCount > countWindow){
//            for (int i = 0;i < serversNo;i++){
//                hlls[i].clear();
//                targetTaskStats[i] = 0;
//            }
//            clearCount = 1;
//        }
//
        if (cleanTime < System.currentTimeMillis()){
            for (int i = 0;i < instanceNum;i++){
                hlls[i].clear();
                targetTaskStats[i] = 0;
            }
            cleanTime += timeWindow;
        }




        int targetTaskId;
        byte[] raw = KeyTransformation.hashCodeGet(key);
        long hashCode = hash[0].hashBytes(raw).asLong();
        int[] choice;
        int counter = 0;
        choice = new int[choiceNumber];
            //int counter = 0;
        while(counter < choiceNumber) {
            choice[counter] =  (int) FastMath.abs(hash[counter].hashBytes(raw).asLong() % instanceNum);
            counter++;
        }
        targetTaskId = selectMinCardinality(choice,hlls);
        targetTaskStats[targetTaskId]++;
        hlls[targetTaskId].addRaw(hashCode);
        return targetTaskId;
    }
    int selectMinCardinality(int[] choice,HLL[] hlls1){
        int index = choice[0];
        long indexCardinality = hlls1[0].cardinality();
        for(int i = 0; i< choice.length; i++) {
            long tmpCardinality = hlls1[choice[i]].cardinality();
            if (tmpCardinality < indexCardinality) {
                index = choice[i];
                indexCardinality = tmpCardinality;

            }
        }
        return index;
    }
}
