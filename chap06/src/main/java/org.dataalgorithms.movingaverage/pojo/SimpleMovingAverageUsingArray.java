package org.dataalgorithms.movingaverage.pojo;

/**
 * @description: 基于数组实现移动平均设计模式
 * @author: fanyeuxiang
 * @createDate: 2020-01-15
 */
public class SimpleMovingAverageUsingArray {

    private double sum = 0;
    private final int period;
    private double[] windows = null;
    private int pointer = 0;
    private int size = 0;

    public SimpleMovingAverageUsingArray(int period) {
        if (period < 1) {
            throw new IllegalArgumentException("period must be > 0");
        }
        this.period = period;
        this.windows = new double[period];
    }

    public void addNewNumber(double number) {
        sum += number;
        if (size < period) {
            windows[pointer++] = number;
            size++;
        }else {
            pointer = pointer % period;
            sum -=  windows[pointer];
            windows[pointer++] = number;
        }
    }

    public double getMovingAverage() {
        if (size == 0) {
            throw new IllegalArgumentException("average is undefined");
        }
        return sum / size;
    }

}
