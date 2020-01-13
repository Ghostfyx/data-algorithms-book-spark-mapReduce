package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @description: 自然键分区器，控制Mapper的键映射到哪个Reducer，此处采用hashcode散列的方式
 *
 * In Hadoop, the partitioning phase takes place after the map() phase
 * and before the reduce() phase
 *
 * @author: fanyeuxiang
 * @createDate: 2020-01-08
 */
public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {
    @Override
    public int getPartition(DateTemperaturePair dateTemperaturePair, Text text, int numberOfPartitions) {
        return Math.abs(dateTemperaturePair.getYearMonth().hashCode() % numberOfPartitions);
    }
}
