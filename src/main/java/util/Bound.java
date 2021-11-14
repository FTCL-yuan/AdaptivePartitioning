package util;

import org.apache.commons.math3.util.FastMath;

import java.util.HashMap;
import java.util.Map;

public class Bound <K> {
    private HashMap<K,Long> highFreqList;
    private double maxHighFreqKeyCount;
//    private double minHighFreqKeyCount;
    private double highFreqKeyTotalCount;
    private int instanceNum;
    private double maxImbalanceTheta;
    private long[] lowFreqKeyLoad;
    private long[] lowFreqKeyResidueLoad;
    private long[] lowFreqKeySplitLoad;
    private int[] sortedLowFreqKeyLoadIndex;
    private long[] sortedLowFreqKeyTotalLoad;
    private double lowFreqKeyTotalCount;
    private double balancedCount;
    private double totalCount;
    private double totalSplitCount;

    private int proposedChoiceNumberOfHighFreq;

    private double curDelta;

    public Bound(HashMap<K, Long> highFreqList, int instanceNum, long[] lowFreqKeyLoad, long[] lowFreqKeyResidueLoad, int[] sortedLowFreqKeyLoadIndex, double balancedCount, double maxImbalanceTheta , double curDelta) {
        this.highFreqList = highFreqList;
        this.instanceNum = instanceNum;
        this.lowFreqKeyLoad = lowFreqKeyLoad;
        this.lowFreqKeyResidueLoad = lowFreqKeyResidueLoad;
        this.sortedLowFreqKeyLoadIndex = sortedLowFreqKeyLoadIndex;
        this.balancedCount = balancedCount;
        this.totalCount = balancedCount * instanceNum;
        this.maxImbalanceTheta = maxImbalanceTheta;
        this.curDelta = curDelta;
        this.sortedLowFreqKeyTotalLoad = new long[instanceNum];
        this.lowFreqKeySplitLoad = new long[instanceNum];


    }

    public void run(){
        int tmpHighFreqChoiceNumber;
        int averageChoiceNumber;
        int tmpKeyNo;
        setMaxMinFreq();
        tmpHighFreqChoiceNumber = getLowerBoundOfMaxMinFreq();
        tmpKeyNo = getLowerBoundOfKeyNo(highFreqList.size(),instanceNum);
        averageChoiceNumber = getAverageChoiceNumber();
        if (tmpKeyNo > averageChoiceNumber){
            averageChoiceNumber = tmpKeyNo;
        }
        if (tmpHighFreqChoiceNumber > averageChoiceNumber)
            this.proposedChoiceNumberOfHighFreq = tmpHighFreqChoiceNumber;
        else
            this.proposedChoiceNumberOfHighFreq = averageChoiceNumber;
    }

    int getAverageChoiceNumber() {
        double tmpTotalCount = 0;
        int tmpChoice1 = 3;
        int tmpChoice2 = 3;
       double tmpHigh = highFreqKeyTotalCount;
        double tmpLnHigh = FastMath.log(tmpHigh);

        double currentSplitLoad = 0;
        double currentResidueLoad = 0;

        for (int i = 0; i < instanceNum - 1; i++) {
            int instance = sortedLowFreqKeyLoadIndex[i];
            long instanceLoad = lowFreqKeyLoad[instance];
            long instanceResidueLoad = lowFreqKeyResidueLoad[instance];
            long instanceSplitLoad = instanceLoad - instanceResidueLoad;
            double tmp1;
            currentResidueLoad += instanceResidueLoad;
            currentSplitLoad += instanceSplitLoad;
            double ln1 = FastMath.log((double)(i + 1) / instanceNum);
            long currentTotalLoad = sortedLowFreqKeyTotalLoad[i];
            double delta = (1 + maxImbalanceTheta - currentTotalLoad / totalCount) * curDelta * (i + 1) / instanceNum;
            tmp1 = (FastMath.log((i + 1) * balancedCount * (1 + maxImbalanceTheta) - delta * totalCount - (currentResidueLoad + currentSplitLoad * (i + 1) / instanceNum)) - tmpLnHigh) / ln1;
            if (Double.isNaN(tmp1)){
                tmp1 = instanceNum;
            }
            if (tmp1 > tmpChoice1) {
                tmpChoice1 = (int) FastMath.ceil(tmp1);
            }
        }
//        long currentTotalLoad = 0;
        currentSplitLoad = totalSplitCount;
        for (int i = 0; i < instanceNum - 1; i++) {
            double tmp2;
            int instance = sortedLowFreqKeyLoadIndex[instanceNum - i - 1];
            long instanceSplitLoad = lowFreqKeySplitLoad[instance];
            currentSplitLoad -= instanceSplitLoad;
            double currentTotalLoad1 = sortedLowFreqKeyTotalLoad[instanceNum - i - 2];
            double currentTotalLoad2 = lowFreqKeyTotalCount - currentTotalLoad1;
            double ln2 = FastMath.log(1 - (double)(i + 1) / instanceNum);
           double delta = ((tmpHigh + currentTotalLoad2 + currentTotalLoad1 * (i + 1) / instanceNum)/totalCount - (i + 1) * (1 - maxImbalanceTheta)/instanceNum)*curDelta;
            tmp2 = (FastMath.log(currentTotalLoad2 + currentSplitLoad * (i + 1) / instanceNum + tmpHigh - (i + 1) * balancedCount * (1 - maxImbalanceTheta) - delta * totalCount) - tmpLnHigh) / ln2;
            if (Double.isNaN(tmp2)){
                tmp2 = instanceNum;
            }
            if (tmp2 > tmpChoice2) {
                tmpChoice2 = (int) FastMath.ceil(tmp2);
            }
        }
        return Integer.min(Integer.max(tmpChoice1, tmpChoice2), instanceNum);

    }
    int getLowerBoundOfMaxMinFreq(){
        double otherFreqBalancedCount;
        int minM;
        int choice;
        otherFreqBalancedCount = (balancedCount * (1 + maxImbalanceTheta) - lowFreqKeyTotalCount/ instanceNum) * (1 - curDelta);
        if (otherFreqBalancedCount <= 0){
            choice = instanceNum;
        }else {
            minM = (int) FastMath.ceil(maxHighFreqKeyCount /otherFreqBalancedCount);
            if (minM >= instanceNum){
                choice = instanceNum;
            }else {
                choice = getLowerBoundOfFreq(minM,instanceNum);
            }
        }
        return choice;
    }
    public static int getLowerBoundOfKeyNo(int m,int n){
        double tmp = FastMath.log(n)/(m*(FastMath.log(n) - FastMath.log(n -1)));
        return (int) FastMath.floor(tmp)+1;
    }
    public static int getLowerBoundOfFreq(int m,int n){
        double tmp = (getLnCnk(n,m -1) + FastMath.log(n))/(FastMath.log(n) - FastMath.log(m -1));
        return (int) FastMath.floor(tmp)+1;
    }
    public static double getLnCnk(long n,long k) {
        if (k > n / 2)
            k = n - k;
        double tmp1 = 0;
        double tmp2 = 0;
        for (long i = n; i > n - k; i--)
            tmp1 += FastMath.log(i);
        for (long i = 1; i < k + 1; i++)
            tmp2 += FastMath.log(i);
        return tmp1 - tmp2;
    }
    void setMaxMinFreq(){
        double maxCount = 0;
        double minCount = balancedCount * instanceNum;
        double totalCount = 0;
        for (Map.Entry entry : highFreqList.entrySet()) {
            //K tmpKey = (K) entry.getKey();
            long tmpCount = (long) entry.getValue();
            totalCount += tmpCount;
            if (tmpCount > maxCount)
                maxCount = tmpCount;
            if (tmpCount < minCount)
                minCount = tmpCount;
        }
        this.maxHighFreqKeyCount = maxCount;
//        this.minHighFreqKeyCount = minCount;
        this.highFreqKeyTotalCount = totalCount;
        long tmpTotalLoad = 0;
        long tmpTotalSplitLoad = 0;
        for (int i = 0;i < instanceNum;i++){
            int tmpIndex = sortedLowFreqKeyLoadIndex[i];
            long tmpLoad = lowFreqKeyLoad[tmpIndex];
            long tmpResidueLoad = lowFreqKeyResidueLoad[tmpIndex];
            long tmpSplitLoad = tmpLoad - tmpResidueLoad;
            lowFreqKeySplitLoad[tmpIndex] = tmpSplitLoad;
            tmpTotalLoad += tmpLoad;
            tmpTotalSplitLoad += tmpSplitLoad;
            sortedLowFreqKeyTotalLoad[i] = tmpTotalLoad;
        }
        this.lowFreqKeyTotalCount = sortedLowFreqKeyTotalLoad[instanceNum - 1];
        this.totalSplitCount = tmpTotalSplitLoad;

    }


    public int getProposedChoiceNumberOfHighFreq() {
        return proposedChoiceNumberOfHighFreq;
    }

}
