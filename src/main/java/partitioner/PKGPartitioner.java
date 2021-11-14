package partitioner;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class PKGPartitioner implements Partitioner {
    private HashFunction h1 = Hashing.murmur3_128(13);
    private HashFunction h2 = Hashing.murmur3_128(17);
    private long[] targetTaskStats;
    @Override
    public int partition(Object key, int numPartitions) {
        if(targetTaskStats==null){
            targetTaskStats = new long[numPartitions];
        }
        byte[] raw;
        ByteBuffer out = ByteBuffer.allocate(4);
        if (key != null) {
            if (key instanceof List) {
                out.putInt(Arrays.deepHashCode(((List)key).toArray()));
            } else if (key instanceof Object[]) {
                out.putInt(Arrays.deepHashCode((Object[])key));
            } else if (key instanceof byte[]) {
                out.putInt(Arrays.hashCode((byte[]) key));
            } else if (key instanceof short[]) {
                out.putInt(Arrays.hashCode((short[]) key));
            } else if (key instanceof int[]) {
                out.putInt(Arrays.hashCode((int[]) key));
            } else if (key instanceof long[]) {
                out.putInt(Arrays.hashCode((long[]) key));
            } else if (key instanceof char[]) {
                out.putInt(Arrays.hashCode((char[]) key));
            } else if (key instanceof float[]) {
                out.putInt(Arrays.hashCode((float[]) key));
            } else if (key instanceof double[]) {
                out.putInt(Arrays.hashCode((double[]) key));
            } else if (key instanceof boolean[]) {
                out.putInt(Arrays.hashCode((boolean[]) key));
            } else {
                out.putInt(key.hashCode());
            }

        } else {
            out.putInt(0);
        }
        raw = out.array();
        //System.out.println(Thread.currentThread().getId()+":"+key.toString()+"::"+raw+":::"+raw.length);
        int firstChoice = (int) (Math.abs(h1.hashBytes(raw).asLong()) % numPartitions);
        int secondChoice = (int) (Math.abs(h2.hashBytes(raw).asLong()) % numPartitions);
        //System.out.println(Thread.currentThread().getId()+":"+key+"::"+firstChoice+":::"+secondChoice);
        int selected = targetTaskStats[firstChoice] > targetTaskStats[secondChoice] ? secondChoice : firstChoice;
        targetTaskStats[selected]++;
        return selected;
    }
}
