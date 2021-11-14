package util;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;

import java.util.HashMap;
import java.util.List;

public class CountSlidingWindowStreamSummary<T> {
    public int k;
    private long windowSize;
    private long usedSize;
    private int capacity;
    private int coDomain;
    private long[][] totalDistribution;
    private StreamSummary<T>[] totalStreamSummary;
    private long count;
    private long[] windowCount;
    public CountSlidingWindowStreamSummary(int k, long size, int capacity , int coDomain) {
        this.k = k;
        this.windowSize = size;
        this.capacity = capacity;
        this.coDomain = coDomain;
        this.totalDistribution = new long[k][];
        this.totalStreamSummary = new StreamSummary[k];
        this.windowCount = new long[k];
        this.count = 0;
        this.usedSize = windowSize/(k -1);
        for (int i = 0; i < k; i++){
            totalDistribution[i] = new long[coDomain];
            totalStreamSummary[i] = new StreamSummary<>(this.capacity);
            windowCount[i] = 0;
        }
    }

    public void offer(T item,int hashCode){
        int windowCounter = (int) (count / usedSize);
        for (int i = 0; i < k; i++){
            if (i != windowCounter){
                totalDistribution[i][hashCode]++;
                totalStreamSummary[i].offer(item);
                windowCount[i]++;
            }
        }
        if (++count % usedSize == 0 ){
            int reCreateWindowCounter = (int) (count / usedSize) - 1;
            totalDistribution[reCreateWindowCounter] = new long[coDomain];
            totalStreamSummary[reCreateWindowCounter] = new StreamSummary<>(this.capacity);
            windowCount[reCreateWindowCounter] = 0;
        }
        count = count % (usedSize*k);
    }
    public HashMap<T,Long> getTopK(float probability) {
        HashMap<T,Long> returnList = new HashMap<T,Long>();
        int windowCounter = (int) (count / usedSize);
        List<Counter<T>> counters = totalStreamSummary[windowCounter].topK(this.capacity);

        for(Counter<T> counter : counters)  {
            Long count = counter.getCount();
            Long error = counter.getError();
            Long freq = count - error;
            float itemProb = (float)freq/windowCount[windowCounter];
            if (itemProb > probability) {
                returnList.put(counter.getItem(),freq);
            }

        }
        return returnList;

    }

    public double getTopKCount(float probability){
        double totalCount = 0.0;
        int windowCounter = (int) (count / usedSize);
        List<Counter<T>> counters = totalStreamSummary[windowCounter].topK(this.capacity);

        for(Counter<T> counter : counters)  {
            float freq = counter.getCount();
            float error = counter.getError();
            float itemProb = (freq-error)/windowCount[windowCounter];
            if (itemProb > probability) {
                totalCount = totalCount + counter.getCount();
            }
        }
        return totalCount;
    }
    public long[] getKeyDistribution(){
        int windowCounter = (int) (count / usedSize);
        long[] keyDistribution = totalDistribution[windowCounter];
        return keyDistribution;
    }



}
