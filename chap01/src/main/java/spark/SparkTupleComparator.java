package spark;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @description: spark tuple2定制比较器
 * @author: fanyeuxiang
 * @createDate: 2020-01-08
 */
public class SparkTupleComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {

    public static final SparkTupleComparator INSTANCE = new SparkTupleComparator();

    private SparkTupleComparator() {
    }

    @Override
    public int compare(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) {
        return t1._1.compareTo(t2._1);
    }
}
