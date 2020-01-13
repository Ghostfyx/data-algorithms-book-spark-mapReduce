package spark;


import org.apache.spark.Partitioner;
import scala.Tuple2;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-09 14:51
 */
public class CustomPartitioner extends Partitioner {

    private final int numPartitions;

    public CustomPartitioner(int partitions) {
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
            Tuple2<String, Integer> tuple2 = (Tuple2<String, Integer>) key;
            return Math.abs(tuple2._1.hashCode() % numPartitions);
        } else {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }
}
