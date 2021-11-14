package partitioner;

import com.clearspring.analytics.stream.StreamSummary;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;
import util.KeyTransformation;
import util.StreamSummaryHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class WChoicePartitioner implements Partitioner {

   // private List<Integer> targetTasks;
    private  long[] targetTaskStats;
    private HashFunction h1 = Hashing.murmur3_128(13);
    private HashFunction h2 = Hashing.murmur3_128(17);
    private StreamSummary<String> streamSummary;


    private long totalItems;

    private int currentTargetTaskNum = 0;
    @Override
    public int partition(Object key, int numPartitions) {
        if (currentTargetTaskNum != numPartitions){
            currentTargetTaskNum = numPartitions;
            targetTaskStats = new long[currentTargetTaskNum];
            streamSummary = new StreamSummary<String>(StreamSummaryHelper.capacity);
            totalItems = 0;
        }

        byte[] raw;
        raw = KeyTransformation.hashCodeGet(key);
        Integer returnId = 0;
        List<Integer> boltIds = new ArrayList<Integer>();
        StreamSummaryHelper ssHelper = new StreamSummaryHelper();
        String str = key.toString();
        totalItems++;
        if(str.isEmpty())
            //boltIds.add(targetTasks.get(0));
            //return 0;
            returnId = 0;
        else
        {
            streamSummary.offer(str);
            float probability = 1/(float)(currentTargetTaskNum*5);
            HashMap<String,Long> freqList = ssHelper.getTopK(streamSummary,probability,totalItems);
            if (totalItems % 10000 == 0){
                System.out.println(freqList);
            }

            //System.out.println("totalItems"+totalItems);
//            System.out.println(freqList);
            //HashMap<String,Long> freqList = slidingWindowStreamSummary.getTopK(probability);
            if(freqList.containsKey(str)) {
                int choice[] = new int[currentTargetTaskNum];
                int count = 0;
                while(count < currentTargetTaskNum) {
                    choice[count] = count;
                    count++;
                }
                int selected = selectMinChoice(targetTaskStats,choice);
                returnId = selected;
                //boltIds.add(targetTasks.get(selected));
                targetTaskStats[selected]++;

            }else {

                int firstChoice = (int) (FastMath.abs(h1.hashBytes(raw).asLong()) % currentTargetTaskNum);
                int secondChoice = (int) (FastMath.abs(h2.hashBytes(raw).asLong()) % currentTargetTaskNum);
                int selected = targetTaskStats[firstChoice]>targetTaskStats[secondChoice]?secondChoice:firstChoice;
                returnId = selected;
                //boltIds.add(targetTasks.get(selected));
                targetTaskStats[selected]++;
            }
        }



        return returnId;
    }
    int selectMinChoice(long loadVector[], int choice[]) {
        int index = choice[0];
        for(int i = 0; i< choice.length; i++) {
            if (loadVector[choice[i]]<loadVector[index])
                index = choice[i];
        }
        return index;
    }
}
