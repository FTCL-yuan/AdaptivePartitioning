package partitioner;

import com.clearspring.analytics.stream.StreamSummary;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import util.*;

import java.util.*;

public class AdaptivePartitioner<K> implements Partitioner<K> {
    private double[] targetTaskStats;
    private double[] tmpLargeTaskStats;
    private double[] tmpTotalTaskStats;
    private double [] instanceImbalanceFactor;
    private double [] latestTaskStats;
    private long latestTotalCount;

    private HashFunction[] hash;
    private int instanceNum;
    private double lnInstanceNum;
    private long totalCount;
    private long epochCount;
    private long maxEpochCount;
    private long minEpochCount;
    private Seed seeds;
    private long usedSize;
    private int windowNum;
    private int capacity;
    private Long windowSize;
    private int windowAdaptiveFactor;
    private long initialWindow;
    private long latestWindow;
    private double highTheta;
    private int virtualInstanceFactor;
    private int coDomain;
    private HashMap<K,Long> highFreqList;
    private HashMap<K,Long> epochHighFreqList;
    private HashSet<Integer> lowFreqKeyToSplit;
    private HashMap<Integer,Integer> lowFreqKeyToSplitMore;
    private int highFreqKeyChoices;
    private int epochHighFreqKeyChoices;
    private TimeSlidingWindowStreamSummary<K> timeSlidingWindowStreamSummary;
    private StreamSummary<K> streamSummary;
    private StreamSummary<K> epochStreamSummary;
    private StreamSummaryHelper<K> ssHelper;
    private double maxImbalanceTheta,maxWindowImbalanceTheta;
    private double priorDelta;
    private double incDelta;
    private double curDelta;
    private double decayFactor;

    public AdaptivePartitioner() {
        this.instanceNum = 0;
        maxEpochCount = 100;
        minEpochCount = 40;
        maxImbalanceTheta = 0.01;
        maxWindowImbalanceTheta = maxImbalanceTheta * 20;
        virtualInstanceFactor = 32;
        usedSize = 5000;
        windowNum = 5;
        windowSize = (windowNum - 1)* usedSize;
        windowAdaptiveFactor = 4;
        highFreqKeyChoices = 3;
        priorDelta = 0.5;
        incDelta = 0.01;
        decayFactor = 0.4;

    }
    @Override
    public int partition(K key, int numPartitions) {
        if (instanceNum != numPartitions){
            instanceNum = numPartitions;
            lnInstanceNum = FastMath.log(instanceNum);
            maxEpochCount = (long)FastMath.ceil(2 * lnInstanceNum * instanceNum);
            minEpochCount = instanceNum;
            targetTaskStats = new double[instanceNum];
            tmpLargeTaskStats = new double[instanceNum];
            tmpTotalTaskStats = new double[instanceNum];
            instanceImbalanceFactor = new  double[instanceNum];
            latestTaskStats = new double[instanceNum];
            latestTotalCount = 0;
            for (int i = 0;i < instanceNum;i++) {
                instanceImbalanceFactor[i] = 1.0;
            }
            seeds = new Seed(instanceNum);
            hash = new HashFunction[instanceNum];
            for (int i = 0; i < hash.length; i++) {
                hash[i] = Hashing.murmur3_128(seeds.SEEDS[i]);
            }
            capacity = (int) FastMath.ceil(2 * instanceNum * lnInstanceNum);
            highTheta = 1.0d / (2 * instanceNum);
            coDomain = (int) Math.ceil(virtualInstanceFactor*instanceNum);
            timeSlidingWindowStreamSummary = new TimeSlidingWindowStreamSummary<>(windowNum, windowSize, capacity, coDomain);
            streamSummary = new StreamSummary<>(capacity);
            epochStreamSummary = new StreamSummary<>(capacity);
            ssHelper = new StreamSummaryHelper<>();
            epochCount = 0;
            lowFreqKeyToSplit = new HashSet<>();
            lowFreqKeyToSplitMore = new HashMap<>();
            totalCount = 0;
            initialWindow = (System.currentTimeMillis() / usedSize);
            latestWindow = initialWindow;
            curDelta = priorDelta;
            incDelta = 1.0d/instanceNum;
        }
        int targetTaskId;
        totalCount++;
        long balancedCount = totalCount / instanceNum;
        byte[] raw = KeyTransformation.hashCodeGet(key);
        int tmpHashCode =(int) FastMath.abs( hash[0].hashBytes(raw).asLong() % coDomain);
        streamSummary.offer(key);

        timeSlidingWindowStreamSummary.offer(key,tmpHashCode);
        long currentWindow = (System.currentTimeMillis() / usedSize);
        epochCount++;
        epochStreamSummary.offer(key);
        boolean initialFlag = false;
        if (currentWindow == initialWindow) {
            initialFlag = true;
        }
        if (initialFlag || epochCount >= maxEpochCount ){
            epochHighFreqList = ssHelper.getTopK(epochStreamSummary,highTheta,epochCount);
            if (initialFlag && totalCount % 100 != 0){
                if (totalCount < 100){
                    epochHighFreqKeyChoices = instanceNum;
                }
            }else {
                double epochHighFreqTotalLoad = ssHelper.getPHead(epochStreamSummary,highTheta,epochCount).probability * epochCount;
                double epochLowFreqTotalLoad = epochCount - epochHighFreqTotalLoad;
                double epochBalancedCount = (double) epochCount / instanceNum;
                double otherFreqBalancedCount = (epochBalancedCount * (1 + maxImbalanceTheta) - epochLowFreqTotalLoad / instanceNum ) * (1 - curDelta);
                double maxCount = 0;
                for (Map.Entry entry : epochHighFreqList.entrySet()) {
                    long tmpCount = (long) entry.getValue();
                    if (tmpCount > maxCount)
                        maxCount = tmpCount;
                }
                int choice1 = 3;
                if (otherFreqBalancedCount <= 0) {
                    choice1 = instanceNum;
                }else {
                    choice1 = Bound.getLowerBoundOfFreq((int) FastMath.ceil(maxCount / otherFreqBalancedCount), instanceNum);
                }
                int choice2 = Bound.getLowerBoundOfKeyNo(epochHighFreqList.size(),instanceNum);
                epochHighFreqKeyChoices = Integer.max(3,Integer.max(choice1,choice2));
               if (!initialFlag){
                    if (epochCount >= maxEpochCount){
                        epochCount = 0;
                        epochStreamSummary = new StreamSummary<>(capacity);
                    }
                }
            }
        }
        if (epochCount >= minEpochCount){
            HashMap<K,Long> recentEpochFreqlist = ssHelper.getTopK(epochStreamSummary,highTheta,epochCount);
            epochHighFreqList.putAll(recentEpochFreqlist);
        }
        if (!initialFlag && currentWindow > latestWindow){
            latestWindow = currentWindow;
            highFreqList = timeSlidingWindowStreamSummary.getTopK(highTheta);
            double highFreqTotalLoad = timeSlidingWindowStreamSummary.getTopKCount(highTheta);
            double totalLoad = timeSlidingWindowStreamSummary.getWindowCount();
            double lowFreqTotalLoad;
            double balancedLoad = totalLoad / instanceNum;
            long[] lowFreqKeyDistribution;
            lowFreqKeyDistribution = timeSlidingWindowStreamSummary.getKeyDistribution();
            long balancedCountPerCoDomain = (long) Math.ceil(balancedLoad / virtualInstanceFactor);
            long balancedLatestLoad = latestTotalCount / instanceNum;
           for (Map.Entry entry : highFreqList.entrySet()) {
                K tmpKey = (K) entry.getKey();
                long tmpCount = (long) entry.getValue();
                byte[] tmpRaw = KeyTransformation.hashCodeGet(tmpKey);
                int tmpId = (int) FastMath.abs(hash[0].hashBytes(tmpRaw).asLong() % coDomain);
                lowFreqKeyDistribution[tmpId] -= tmpCount;
            }
            long[] lowFreqKeyLoad = new long[instanceNum];
            HashMap<Integer, Long>[] lowFreqKeyList = new HashMap[instanceNum];
            TreeSet<Integer>[] virtualInstancesByLoad = new TreeSet[instanceNum];
            TreeSet<Integer>[] virtualInstancesByIndex = new TreeSet[instanceNum];
            lowFreqKeyToSplitMore.clear();
            lowFreqKeyToSplit.clear();
            for (int i = 0; i < instanceNum; i++) {
                lowFreqKeyList[i] = new HashMap<>();
                lowFreqKeyLoad[i] = 0;
                for (int j = 0; j < virtualInstanceFactor; j++) {
                    long virtualInstanceLoad = lowFreqKeyDistribution[j * instanceNum + i];
                    lowFreqKeyLoad[i] += virtualInstanceLoad;
                    lowFreqKeyList[i].put(j * instanceNum + i, virtualInstanceLoad);
                }
                virtualInstancesByLoad[i] = new TreeSet<>(new PartitionSorterByLoad<Integer>(lowFreqKeyList[i]));
                virtualInstancesByIndex[i] = new TreeSet<>(new PartitionSorterByIndex(lowFreqKeyList[i]));
                virtualInstancesByLoad[i].addAll(lowFreqKeyList[i].keySet());
                virtualInstancesByIndex[i].addAll(lowFreqKeyList[i].keySet());
            }
            HashMap<Integer, Long> lowFreqKeyLoadMap = new HashMap<>();
            for (int i = 0; i < instanceNum; i++) {
                lowFreqKeyLoadMap.put(i, lowFreqKeyLoad[i]);
            }
            TreeSet<Integer> sortedLowFreqKeyLoadMap = new TreeSet<>(new PartitionSorterByLoad<Integer>(lowFreqKeyLoadMap));
            sortedLowFreqKeyLoadMap.addAll(lowFreqKeyLoadMap.keySet());
            int[] sortedLowFreqKeyLoadIndex = new int[instanceNum];
            long[] sortedLowFreqKeyTotalLoad = new long[instanceNum];
            Iterator<Integer> iterator1 = sortedLowFreqKeyLoadMap.iterator();
            long tmpTotalLoad = 0;
            for (int i = 0; i < instanceNum; i++) {
                if (iterator1.hasNext())
                    sortedLowFreqKeyLoadIndex[i] = iterator1.next();
                long tmpLoad = lowFreqKeyLoad[sortedLowFreqKeyLoadIndex[i]];
                tmpTotalLoad += tmpLoad;
                sortedLowFreqKeyTotalLoad[i] = tmpTotalLoad;
            }
            lowFreqTotalLoad = sortedLowFreqKeyTotalLoad[instanceNum - 1];
            int imbalanceCount = 0;
            for (int i = 0; i < instanceNum; i++) {
                if (latestTaskStats[i] > balancedLatestLoad * (1 + maxWindowImbalanceTheta) || latestTaskStats[i] < balancedLatestLoad * (1 - maxWindowImbalanceTheta) || targetTaskStats[i] > balancedCount * (1 + maxImbalanceTheta) || targetTaskStats[i] < balancedCount * (1 - maxImbalanceTheta)) {
                    imbalanceCount++;
                }
            }
            if (imbalanceCount == 0) {
                curDelta *= decayFactor;
                if (curDelta < priorDelta) {
                    curDelta = priorDelta;
                }
            } else {
                curDelta += incDelta * imbalanceCount;
            }
            curDelta = Double.min(curDelta,1.0);
            double maxLoadThreshold = balancedLoad * (1 + maxImbalanceTheta);
            int tmpInstance = 0;
            while ((tmpInstance < instanceNum) && lowFreqKeyLoad[sortedLowFreqKeyLoadIndex[tmpInstance]] > maxLoadThreshold) {
                tmpInstance++;
            }
            double epsilon = Double.max(1, (double) balancedCountPerCoDomain / 2);
            for (int i = 0; i < instanceNum - 1; i++) {
                double currentTotalLoad = 0;
                double currentSplitLoad = 0;
                double delta;
                currentTotalLoad = sortedLowFreqKeyTotalLoad[i];
                double alpha;
                delta = (1 + maxImbalanceTheta - currentTotalLoad / totalLoad) * curDelta * (i + 1) / instanceNum;
                double maxAlpha = balancedLoad * (i + 1) * (1 + maxImbalanceTheta) - delta * totalLoad;
                do {
                    if (i < tmpInstance) {
                        currentSplitLoad = currentTotalLoad - (i + 1) * maxLoadThreshold;
                    } else {
                        if (tmpInstance == 0) {
                            currentSplitLoad = 0;
                        } else {
                            currentSplitLoad = sortedLowFreqKeyTotalLoad[tmpInstance - 1] - (tmpInstance - 1) * maxLoadThreshold;
                        }
                    }
                    alpha = currentTotalLoad - currentSplitLoad + (double)(i + 1) / instanceNum * currentSplitLoad;
                    if (alpha > maxAlpha) {
                        maxLoadThreshold -= epsilon;
                        while ((tmpInstance < instanceNum) && lowFreqKeyLoad[sortedLowFreqKeyLoadIndex[tmpInstance]] > maxLoadThreshold) {
                            tmpInstance++;
                        }
                    }
                } while (alpha > maxAlpha && tmpInstance < instanceNum);
            }
            for (int i = 0; i < instanceNum - 1; i++) {
                double currentTotalLoad1;
                double currentTotalLoad2;
                double currentSplitLoad;
                double delta;
                currentTotalLoad1 = sortedLowFreqKeyTotalLoad[instanceNum - i - 2];
                currentTotalLoad2 = lowFreqTotalLoad - currentTotalLoad1;
                delta = ((highFreqTotalLoad + currentTotalLoad2 + currentTotalLoad1 * (i + 1) / instanceNum)/totalLoad - (i + 1) * (1 - maxImbalanceTheta))*curDelta;
                double omega;
                double minOmega = balancedLoad * (i + 1) * (1 - maxImbalanceTheta) + delta * totalLoad;
                do {
                    if (numPartitions - 2 - i < tmpInstance) {
                        currentSplitLoad = currentTotalLoad1 - (numPartitions - 1 - i) * maxLoadThreshold;
                    } else {
                        if (tmpInstance == 0) {
                            currentSplitLoad = 0;
                        } else {
                            currentSplitLoad = sortedLowFreqKeyTotalLoad[tmpInstance - 1] - tmpInstance * maxLoadThreshold;
                        }
                    }
                    omega = currentTotalLoad2 + currentSplitLoad * (double) (i + 1) / numPartitions + (highFreqTotalLoad);
                    if (omega < minOmega) {
                        maxLoadThreshold -= epsilon;
                        while ((tmpInstance < instanceNum) && (lowFreqKeyLoad[sortedLowFreqKeyLoadIndex[tmpInstance]] > maxLoadThreshold)) {
                            tmpInstance++;
                        }
                    }
                } while (omega < minOmega && tmpInstance < instanceNum);
            }
            if (maxLoadThreshold < 0) {
                maxLoadThreshold = 0;
            }
            long[] lowFreqKeyResidueLoad = new long[instanceNum];
            for (int i = 0; i < instanceNum; i++) {
                int instance = sortedLowFreqKeyLoadIndex[i];
                long instanceLoad = lowFreqKeyLoad[instance];
                double splitThreshold = maxLoadThreshold;
                Iterator<Integer> iteratorByLoad = virtualInstancesByLoad[instance].iterator();
                Iterator<Integer> iteratorByIndex = virtualInstancesByIndex[instance].iterator();
                boolean loadOrIndex = true;
                while (instanceLoad > splitThreshold && iteratorByLoad.hasNext() && iteratorByIndex.hasNext()) {
                    if (loadOrIndex) {
                        int virtualInstance = iteratorByLoad.next();
                        long tmpLoad = lowFreqKeyList[instance].get(virtualInstance);
                        if (tmpLoad > 2 * balancedCountPerCoDomain) {
                            int choiceNum = (int) FastMath.ceil((double) tmpLoad / balancedCountPerCoDomain);
                            lowFreqKeyToSplitMore.put(virtualInstance, choiceNum);
                            instanceLoad -= tmpLoad;
                        } else {
                            loadOrIndex = false;
                        }
                    }
                    if (!loadOrIndex) {
                        int virtualInstance = iteratorByIndex.next();
                        while (lowFreqKeyToSplitMore.containsKey(virtualInstance) && iteratorByIndex.hasNext()) {
                            iteratorByIndex.next();
                        }
                        long tmpCount = lowFreqKeyList[instance].get(virtualInstance);
                        instanceLoad -= tmpCount;
                        lowFreqKeyToSplit.add(virtualInstance);
                    }
                }
                lowFreqKeyResidueLoad[instance] = instanceLoad;
            }
            for (int i = 0;i < instanceNum;i++){
                int virtualInstanceNumber = (int) FastMath.ceil(curDelta * virtualInstanceFactor);
                for (int j = 0;j < virtualInstanceNumber;j++) {
                    lowFreqKeyToSplit.add(i + j * instanceNum);
                }
            }
            if (highFreqList.size() != 0) {
                Bound bound = new Bound(highFreqList, instanceNum, lowFreqKeyLoad, lowFreqKeyResidueLoad, sortedLowFreqKeyLoadIndex, balancedLoad, maxImbalanceTheta,curDelta);
                bound.run();
                highFreqKeyChoices = bound.getProposedChoiceNumberOfHighFreq();
            }
            for (int i = 0; i < instanceNum; i++) {
                latestTaskStats[i] = 0;
            }
            latestTotalCount = 0;
            if (currentWindow - initialWindow > 2 * (windowNum - 1)) {
                int windowNum2,windowNum3;
                long usedSize1;
                windowNum2 = windowNum3 = windowNum;
                usedSize1 = usedSize;
                boolean windowChangeFlag  = false;
                double countTheta1 = windowAdaptiveFactor / maxWindowImbalanceTheta;
                if (balancedLatestLoad < countTheta1) {
                    usedSize1 = 1000 * (long)FastMath.ceil(countTheta1 * usedSize/ (balancedLatestLoad * 1000));
                    windowChangeFlag = true;
                }
                double countTheta2 = 2 * maxEpochCount;
                if (totalLoad * usedSize1 / usedSize < countTheta2 ) {
                    windowNum2 = (int) FastMath.ceil(countTheta2 / (totalLoad * usedSize1 / usedSize) / (windowNum - 1)) + 1;
                    windowChangeFlag = true;
                }

                double countTheta3 = windowAdaptiveFactor / maxImbalanceTheta;
                if (balancedLoad  * usedSize1 / usedSize < countTheta3) {
                    windowNum3 = (int) FastMath.ceil(countTheta3 / (balancedLoad * usedSize1 / usedSize) * (windowNum - 1)) + 1;
                    windowChangeFlag = true;
                }

                if (balancedLatestLoad > 4 * countTheta1){
                    usedSize1 = 1000 * Long.max(1,(long)FastMath.ceil(usedSize / 2000d));
                }
                if (totalLoad * usedSize1 / usedSize >= 4 * countTheta2 && balancedLoad * usedSize1 / usedSize  >= 4 * countTheta3) {
                    windowNum = Integer.max(1,(int) FastMath.ceil((windowNum - 1) / 2d)) + 1;
                    windowChangeFlag = true;
                }
                if (windowChangeFlag) {
                    usedSize = usedSize1;
                    windowNum = Integer.max(windowNum,Integer.max(windowNum2,windowNum3));
                    windowSize = (windowNum -1) * usedSize;
                    initialWindow = (System.currentTimeMillis() / usedSize);
                    latestWindow = initialWindow;
                    timeSlidingWindowStreamSummary = new TimeSlidingWindowStreamSummary<>(windowNum, windowSize, capacity, coDomain);
                }
            }
        }

        int choiceNumber =1;
        int virtualInstance = (int) FastMath.abs(hash[0].hashBytes(raw).asLong() % coDomain);
        if (lowFreqKeyToSplit != null && lowFreqKeyToSplit.contains(virtualInstance)) {
            choiceNumber = Integer.max(choiceNumber,2);
        }
        if (lowFreqKeyToSplitMore != null && lowFreqKeyToSplitMore.containsKey(virtualInstance)) {
            choiceNumber = Integer.max(choiceNumber,lowFreqKeyToSplitMore.get(virtualInstance));
        }
        if (highFreqList != null && highFreqList.containsKey(key)) {
            choiceNumber = Integer.max(choiceNumber,highFreqKeyChoices);
        }
        if (epochHighFreqList != null && epochHighFreqList.containsKey(key)) {
            choiceNumber = Integer.max(choiceNumber,epochHighFreqKeyChoices);
        }
        if (initialFlag){
            choiceNumber = Integer.max(choiceNumber,2);
        }

        int[] choice;
        int counter = 0;
        if (choiceNumber < instanceNum) {
            choice = new int[choiceNumber];
            while (counter < choiceNumber) {
                choice[counter] = (int) FastMath.abs(hash[counter].hashBytes(raw).asLong() % instanceNum);
                counter++;
            }
        } else {
            choice = new int[instanceNum];
            while (counter < instanceNum) {
                choice[counter] = counter;
                counter++;
            }
        }
        targetTaskId = KeyTransformation.selectMinChoice(targetTaskStats, choice);
        if ((highFreqList != null && highFreqList.containsKey(key)) || (epochHighFreqList != null && epochHighFreqList.containsKey(key))) {
            tmpLargeTaskStats[targetTaskId]++;
            tmpTotalTaskStats[targetTaskId]++;
        }
        latestTotalCount++;
        latestTaskStats[targetTaskId]++;
        targetTaskStats[targetTaskId]++;
        return targetTaskId;
    }
    class PartitionSorterByLoad<K> implements Comparator<K> {

        Map<K, Long> base;
        public PartitionSorterByLoad(Map<K, Long> base) {
            this.base = base;
        }
        public int compare(K a, K b) {
            if (base.get(a) > base.get(b)) {
                return -1;
            } else if (base.get(a) < base.get(b)) {
                return 1;
            } else {
                if (a instanceof Integer){
                    if (Integer.parseInt(a.toString()) < (Integer.parseInt(a.toString()))) {
                        return -1;
                    } else if (Integer.parseInt(a.toString())> (Integer.parseInt(a.toString()))) {
                        return 1;
                    }
                }else {
                    if (a.hashCode() < b.hashCode()) {
                        return -1;
                    } else if (a.hashCode() > b.hashCode()) {
                        return 1;
                    }
                }
                return 0;
            }
        }
    }
    class PartitionSorterByIndex implements Comparator<Integer> {

        Map<Integer, Long> base;
        public PartitionSorterByIndex(Map<Integer, Long> base) {
            this.base = base;
        }
        public int compare(Integer a, Integer b) {
            if (a < b) {
                return -1;
            } else if (a > b) {
                return 1;
            }
            return 0;
        }
    }
}
