package partitioner;

import com.clearspring.analytics.stream.StreamSummary;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;
import util.KeyTransformation;
import util.PHeadCount;
import util.Seed;
import util.StreamSummaryHelper;

import java.util.HashMap;

public class DChoicePartitioner implements Partitioner {
    //private List<Integer> targetTasks;
    private  long[] targetTaskStats;
    //WorkerTopologyContext context;
    private HashFunction h1 = Hashing.murmur3_128(13);
    private HashFunction h2 = Hashing.murmur3_128(17);
    private HashFunction[] hash;
    private StreamSummary<String> streamSummary;
    private int instanceNum;
    private Long totalItems;
    private Seed seeds;
    //int tmpd;

    private int currentTargetTaskNum = 0;
    @Override
    public int partition(Object key, int numPartitions) {
        if (currentTargetTaskNum != numPartitions){
            currentTargetTaskNum = numPartitions;
            targetTaskStats = new long[currentTargetTaskNum];
            streamSummary = new StreamSummary<String>(StreamSummaryHelper.capacity);
            totalItems = (long) 0;
            instanceNum = currentTargetTaskNum;
            //SEED.
            //seed = new SEED();
            //seeds = new Seed(serversNe)
            seeds = new Seed(instanceNum);
            hash = new HashFunction[this.instanceNum];
            for (int i=0;i<hash.length;i++) {
                hash[i] = Hashing.murmur3_128(seeds.SEEDS[i]);
            }
            //tmpd = 0;
        }

        byte[] raw;

        raw = KeyTransformation.hashCodeGet(key);
            //List<Integer> boltIds = new ArrayList<Integer>();
        int returnId = 0;
        StreamSummaryHelper<String> ssHelper = new StreamSummaryHelper();
        String str = key.toString();
        totalItems++;
        if(str.isEmpty())
            //boltIds.add(targetTasks.get(0));
            //return 0;
            returnId = 0;
        else
        {
            streamSummary.offer(str);
            float probability = 2/(float)(currentTargetTaskNum*10);
            double epsilon = 0.0001;
            int Choice =2;
            HashMap<String,Long> freqList = ssHelper.getTopK(streamSummary,probability,totalItems);
            if (totalItems % 10000 == 0){
                System.out.println(freqList);
            }
            if(freqList.containsKey(str)) {
                double pTop = ssHelper.getPTop(streamSummary,this.totalItems);
                PHeadCount pHead = ssHelper.getPHead(streamSummary,probability,this.totalItems);
                double pTail = 1-pHead.probability;
                double n = (double)this.instanceNum;
                double val1 = (n-1)/n;
                double d = FastMath.round(pTop*this.instanceNum);
                double val2,val3,val4,sum1;
                double sum2,value1,value2,value3,value4;
                do{
                    //finding sum Head
                    val2 = FastMath.pow(val1, pHead.numberOfElements*d);
                    val3 = 1-val2;
                    val4 = FastMath.pow(val3, 2);
                    sum1 = pHead.probability + pTail*val4;

                    //finding sum1
                    value1 = FastMath.pow(val1, d);
                    value2 = 1-value1;
                    value3 = FastMath.pow(value2, d);
                    value4 = FastMath.pow(value2, 2);
                    sum2 = pTop+((pHead.probability-pTop)*value3)+(pTail*value4);
                    d++;
                }while((d<=this.instanceNum) && ((sum1 > (val3+epsilon)) || (sum2 > (value2+epsilon))));

                Choice = (int)d-1;

                if (totalItems < 1000){
                    Choice = 2;
                }
                //Hash the key accordingly
                int counter = 0;
                int[] choice;
                //byte[] b = str.toString().getBytes();
                byte[] b = raw;
                if(Choice < this.instanceNum) {
                    choice = new int[Choice];
                    while(counter < Choice) {
                        choice[counter] =  FastMath.abs(hash[counter].hashBytes(b).asInt()%instanceNum);
                        counter++;
                    }
                }else {
                    choice = new int[this.instanceNum];
                    while(counter < this.instanceNum) {
                        choice[counter] =  counter;
                        counter++;
                    }
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
    public int selectMinChoice(long loadVector[], int choice[]) {
        int index = choice[0];
        for(int i = 0; i< choice.length; i++) {
            if (loadVector[choice[i]]<loadVector[index])
                index = choice[i];
        }
        return index;
    }
}
