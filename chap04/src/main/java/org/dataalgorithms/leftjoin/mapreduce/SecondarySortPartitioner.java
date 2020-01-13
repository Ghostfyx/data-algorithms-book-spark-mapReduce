package org.dataalgorithms.leftjoin.mapreduce;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @description: 分区函数，将相同的Key发送到一个Reducer处理
 * @author: fanyeuxiang
 * @createDate: 2020-01-13 13:59
 */
public class SecondarySortPartitioner extends Partitioner<PairOfStrings, Object> {

    @Override
    public int getPartition(PairOfStrings pairOfStrings, Object o, int i) {
        // &按位与的运算规则是将两边的数转换为二进制位，然后运算最终值
        return (pairOfStrings.getLeftElement().hashCode() & Integer.MAX_VALUE) % i;
    }
}
