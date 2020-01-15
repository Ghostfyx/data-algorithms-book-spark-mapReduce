package org.dataalgorithms.movingaverage.pojo;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @description: 基于队列实现移动平均设计模式
 * @author: fanyeuxiang
 * @createDate: 2020-01-15
 */
public class SimpleMovingAverage {

    /**
     * 当前窗口数据统计量总和
     */
    private double sum = 0.0;

    /**
     * 窗口大小
     */
    private final int period;

    /**
     * 时间窗口数据结构
     */
    private final Queue<Double> windows = new LinkedList<>();

    public SimpleMovingAverage(int period) {
        if (period < 1) {
            throw new IllegalArgumentException("period must be > 0");
        }
        this.period = period;
    }

    public void addNewNumber(double number) {
        windows.add(number);
        sum += number;
        if (windows.size() > period){
            sum -= windows.remove();
        }
    }

    public double getMovingAverage() {
        if (windows.isEmpty()) {
            throw new IllegalArgumentException("average is undefined");
        }
        return sum / windows.size();
    }
}
