package org.dataalgorithms.movingaverage.spark;

import org.apache.spark.Partitioner;
import scala.Tuple2;

/**
 * @Description: Spark组合键分区器
 * @Author: FanYueXiang
 * @Date: 2020/1/15 11:26 PM
 */
public class MovingAveragePartitioner extends Partitioner {

    private final int numPartitions;

    public MovingAveragePartitioner(int partitions) {
        assert (partitions > 0);
        this.numPartitions = partitions;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        if (key == null) {
            return 0;
        } else if (key instanceof Tuple2) {
            @SuppressWarnings("unchecked")
            Tuple2<String, Long> tuple2 = (Tuple2<String, Long>) key;
            return Math.abs((int)(hash(tuple2._1 )% numPartitions));
        } else {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    /**
     * adapted from String.hashCode()
     */
    static long hash(String str) {
        long h = 1125899906842597L; // prime
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31 * h + str.charAt(i);
        }
        return h;
    }
}
