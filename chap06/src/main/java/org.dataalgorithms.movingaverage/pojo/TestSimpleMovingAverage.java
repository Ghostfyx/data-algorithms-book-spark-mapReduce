package org.dataalgorithms.movingaverage.pojo;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-15
 */
public class TestSimpleMovingAverage {

    public static void main(String[] args){
        // time series        1   2   3  4   5   6   7
        double[] testData = {10, 18, 20, 30, 24, 33, 27};
        int[] allWindowSizes = {3, 4};
        for (int windowSize : allWindowSizes){
            SimpleMovingAverage simpleMovingAverage = new SimpleMovingAverage(windowSize);
            for (int i = 0; i < testData.length; i++){
                simpleMovingAverage.addNewNumber(testData[i]);
                System.out.println("current day: "+(i+1)+", SMA ="+simpleMovingAverage.getMovingAverage());
            }
        }
    }

}
