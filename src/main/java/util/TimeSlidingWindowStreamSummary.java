package util;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;

import java.util.HashMap;
import java.util.List;

public class TimeSlidingWindowStreamSummary<T> {
    private int windowNum;
    private long windowSize;
    private long usedSize;
    private int capacity;
    private int coDomain;
    private long[][] totalDistribution;
    private StreamSummary<T>[] totalStreamSummary;
    private long[] windowCount;
    private int usedWindow;

    public TimeSlidingWindowStreamSummary(int windowNum, long size, int capacity ,int coDomain) {
        this.windowNum = windowNum;
        this.windowSize = size;
        this.capacity = capacity;
        this.coDomain = coDomain;
        this.totalDistribution = new long[windowNum][];
        this.totalStreamSummary = new StreamSummary[windowNum];
        this.windowCount = new long[windowNum];
        this.usedSize = windowSize/(windowNum -1);
        for (int i = 0; i < windowNum; i++){
            totalDistribution[i] = new long[coDomain];
            totalStreamSummary[i] = new StreamSummary<>(this.capacity);
            windowCount[i] = 0;
        }
        this.usedWindow = (int) ((System.currentTimeMillis() / usedSize) % windowNum);
    }

    public void offer(T item,int hashCode){
        long currentTime = System.currentTimeMillis();
        int currentWindow = (int) ((currentTime / usedSize) % windowNum);
        if (currentWindow != usedWindow){
            totalDistribution[usedWindow] = new long[coDomain];
            totalStreamSummary[usedWindow] = new StreamSummary<>(this.capacity);
            windowCount[usedWindow] = 0;
            usedWindow = currentWindow;
        }
        for (int i = 0; i < windowNum; i++){
            if (i != usedWindow){
                totalDistribution[i][hashCode]++;
                totalStreamSummary[i].offer(item);
                windowCount[i]++;
            }
        }

    }
    public HashMap<T,Long> getTopK(double probability) {
        HashMap<T,Long> returnList = new HashMap<>();
        int windowCounter = usedWindow;
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

    public double getTopKCount(double probability){
        double totalCount = 0.0;
        int windowCounter = usedWindow;
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
        int windowCounter = usedWindow;
        return totalDistribution[windowCounter];
    }
    public long getWindowCount(){
        int windowCounter = usedWindow;
        return windowCount[windowCounter];
    }
}
