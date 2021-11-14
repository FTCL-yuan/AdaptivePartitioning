package partitioner;

import org.apache.flink.api.common.functions.Partitioner;

public class ShufflePartitionerRoundRobin<K> implements Partitioner<K> {
    //private Random random = new Random();
    private int j = -1;
    @Override
    public int partition(K k, int i) {
        return j = ++j % i;
       // return j;
        //return random.nextInt(i);
        //return 0;
    }
}
