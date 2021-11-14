package partitioner;

import org.apache.commons.math3.util.FastMath;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;
import util.KeyTransformation;
import util.Seed;

public class HashPartitioner<K> implements Partitioner<K>{
    private HashFunction[] hash;
    private int currentTargetTaskNum;
    private int instanceNum;
    private Seed seeds;
    @Override
    public int partition(K key, int numPartitions) {
        if (currentTargetTaskNum != numPartitions) {
            currentTargetTaskNum = numPartitions;
            instanceNum = numPartitions;
            hash = new HashFunction[this.instanceNum];
            seeds = new Seed(instanceNum);
            for (int i = 0; i < hash.length; i++) {
                hash[i] = Hashing.murmur3_128(seeds.SEEDS[i]);
            }
        }
        //System.out.println(Thread.currentThread().getId());
        byte[] raw = KeyTransformation.hashCodeGet(key);
        int targetTaskId = FastMath.abs(hash[0].hashBytes(raw).asInt() % instanceNum);
        return targetTaskId;
        //return  MathUtils.murmurHash(.hashCode()) % i;
        //return 0;
    }
}
