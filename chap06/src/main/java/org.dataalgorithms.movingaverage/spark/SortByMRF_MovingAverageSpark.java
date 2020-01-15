package org.dataalgorithms.movingaverage.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.dataalgorithms.movingaverage.util.DateUtil;
import scala.Tuple2;

/**
 * @Description:
 * @Author: FanYueXiang
 * @Date: 2020/1/15 11:06 PM
 */
public class SortByMRF_MovingAverageSpark {

    public static void main(String[] args){
        if (args.length != 4){
            System.err.println("Usage: SortByMRF_MovingAverageDriver <input> <output> <window_size> <partitions> ");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputPath = args[1];
        int windowSize = Integer.parseInt(args[2]);
        int partitions = Integer.parseInt(args[3]);

        JavaSparkContext ctx = new JavaSparkContext();
        JavaRDD<String> lines = ctx.textFile(inputPath);

        // 二次排序将自然键转换组合键
        JavaPairRDD<Tuple2<String,Long>, Tuple2<Long, Double>> pairRDD = lines.mapToPair((String s) ->{
           String[] tokens = s.split(",");
           String name = tokens[0];
           Long timeStamp = DateUtil.getDateAsMilliSeconds(tokens[1]);
           Double price = Double.valueOf(tokens[2]);
           return new Tuple2<>(new Tuple2<>(name, timeStamp), new Tuple2<>(timeStamp, price));
        });

        // 对组合键进行分区和排序
        JavaPairRDD<Tuple2<String,Long>, Tuple2<Long, Double>> sorted = pairRDD.repartitionAndSortWithinPartitions(new MovingAveragePartitioner(partitions),
                MovingAverageComparator.INSTANCE);

        // 将排序后的组合键，转换为自然键
        JavaPairRDD<String, Tuple2<Long, Double>> natureSorted = sorted.mapToPair((Tuple2<Tuple2<String,Long>, Tuple2<Long, Double>> s) ->{
            return new Tuple2<>(s._1._1, s._2);
        });

    }

}
