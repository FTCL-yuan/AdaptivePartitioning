package partitioner.cardinalityAwarePartitioners;

import net.agkn.hll.HLL;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;
import util.KeyTransformation;
import util.Seed;

public class AMPartitioner <K> implements Partitioner<K> {
    private int choiceNumber;
    private  double[] targetTaskStats;
    private HashFunction[] hash;
    private Seed seeds;
    transient HLL[] hlls;
    private int instanceNum = 0;


    private long countWindow,timeWindow,clearCount,cleanTime;

    public AMPartitioner(int choiceNumber) {
        this.choiceNumber = choiceNumber;
    }
    public AMPartitioner(){
        this.choiceNumber = 2;
    }

    @Override
    public int partition(K key, int numPartitions) {
        if (instanceNum != numPartitions){
            instanceNum = numPartitions;
            seeds = new Seed(instanceNum);
            hash = new HashFunction[this.instanceNum];
            for (int i = 0; i < hash.length; i++) {
                hash[i] = Hashing.murmur3_128(seeds.SEEDS[i]);
            }
            targetTaskStats = new double[instanceNum];
            hlls = new HLL[instanceNum];
            for (int i = 0; i < instanceNum;i++){
                hlls[i] = new HLL(12,5);
                targetTaskStats[i] = 0;
            }
//            countWindow = 1000000;
//            clearCount = 0;
//
            timeWindow = 240000;
            cleanTime = System.currentTimeMillis() + timeWindow;

        }
        //long startTime = System.nanoTime();

//        clearCount++;
//        if (clearCount > countWindow){
//            for (int i = 0;i < serversNo;i++){
//                hlls[i].clear();
//                targetTaskStats[i] = 0;
//            }
//            clearCount = 1;
//        }
//
        if (cleanTime <= System.currentTimeMillis()){
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
        //targetTaskId = selectMinCardinality(choice,hlls);
        //boolean flag = true;
        targetTaskId = -1;
        for (int i = 0;i < choiceNumber;i++){
            try {
                if (contains(hashCode,hlls[choice[i]])){
                    targetTaskId = choice[i];
                    break;
                    //flag = false;
                }
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        }
        if (targetTaskId == -1){
            targetTaskId = selectMinCardinality(choice,hlls);
        }
        targetTaskStats[targetTaskId]++;
        hlls[targetTaskId].addRaw(hashCode);
        //long endTime = System.nanoTime();
        //System.out.println(endTime - startTime);
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
    boolean contains(long key,HLL hll) throws CloneNotSupportedException {
        HLL tmpHll = hll.clone();
        tmpHll.addRaw(key);
        if (tmpHll.cardinality() == hll.cardinality()){
            //System.out.println("contains");
            return true;
        }else {
            //System.out.println("not contains");
            return false;
        }
    }
}
