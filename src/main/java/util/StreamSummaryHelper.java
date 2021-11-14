package util;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;

import java.util.HashMap;
import java.util.List;

public class StreamSummaryHelper<T> {
    public static int capacity = 100;
    public HashMap<T,Long> getTopK(StreamSummary<T> topk, double probability, Long totalItems) {
        HashMap<T,Long> returnList = new HashMap<T,Long>();
        List<Counter<T>> counters = topk.topK(topk.getCapacity());

        for(Counter<T> counter : counters)  {
            float freq = counter.getCount();
            float error = counter.getError();
            float itemProb = (freq+error)/totalItems;
            //float itemProb = freq/totalItems;
            if (itemProb > probability) {
                returnList.put(counter.getItem(),counter.getCount());
            }
        }
        return returnList;
    }
    public PHeadCount getPHead(StreamSummary<T> topk, double probability, Long totalItems) {
        PHeadCount returnValue = new PHeadCount();
        returnValue.probability=0;

        List<Counter<T>> counters = topk.topK(topk.getCapacity());

        for(Counter<T> counter : counters)  {
            float freq = counter.getCount();
            float error = counter.getError();
            float itemProb = (freq+error)/totalItems;
            //float itemProb = freq/totalItems;
            if (itemProb > probability) {
                returnValue.probability+=itemProb;
                returnValue.numberOfElements++;
            }
        }
        return returnValue;
    }
    public float getPTop(StreamSummary<T> topk, Long totalItems) {
        List<Counter<T>> counters = topk.topK(1);
        for(Counter<T> counter : counters)  {
            float freq = counter.getCount();
            float error = counter.getError();
            float itemProb = (freq+error)/totalItems;
            //float itemProb = freq/totalItems;
            return itemProb;
        }
        return 0f;
    }
}
