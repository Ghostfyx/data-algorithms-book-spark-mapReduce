package org.dataalgorithms.topn.spark;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-13 10:23
 */
public class MyTupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

    final static MyTupleComparator INSTANCE = new MyTupleComparator();


    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        // topN 降序排列
        return -o1._2.compareTo(o2._2);
    }
}
