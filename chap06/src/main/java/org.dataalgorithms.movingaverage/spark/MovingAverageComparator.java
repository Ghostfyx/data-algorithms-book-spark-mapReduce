package org.dataalgorithms.movingaverage.spark;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @Description: Spark 实现移动平均组合键排序器
 * @Author: FanYueXiang
 * @Date: 2020/1/15 11:20 PM
 */
public class MovingAverageComparator implements Serializable, Comparator<Tuple2<String, Long>> {

    private static final long serialVersionUID = -3952355959828464800L;

    static final MovingAverageComparator INSTANCE = new MovingAverageComparator();


    @Override
    public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
        if (o1._1.compareTo(o2._1) != 0) {
            return o1._2.compareTo(o2._2);
        } else {
            return o1._1.compareTo(o2._1);
        }
    }
}
