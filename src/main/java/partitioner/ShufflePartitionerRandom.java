package partitioner;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.Random;

public class ShufflePartitionerRandom<K> implements Partitioner<K> {
    private Random random = new Random();
    @Override
    public int partition(K k, int i) {
        return random.nextInt(i);
    }
}
