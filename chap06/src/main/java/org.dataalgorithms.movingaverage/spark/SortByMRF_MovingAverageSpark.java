package org.dataalgorithms.movingaverage.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.dataalgorithms.movingaverage.util.DateUtil;
import scala.Tuple2;

import java.util.*;

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
        Broadcast<Integer> timeWindow = ctx.broadcast(windowSize);
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

        sorted.saveAsTextFile(outputPath + "/1");

        // 将排序后的组合键，转换为自然键
        JavaPairRDD<String, Tuple2<String, Double>> natureSorted = sorted.mapToPair((Tuple2<Tuple2<String,Long>, Tuple2<Long, Double>> s) ->{
            return new Tuple2<>(s._1._1, new Tuple2<>(DateUtil.getDateAsString(s._2._1),s._2._2));
        });

        natureSorted.collect().forEach((Tuple2<String, Tuple2<String, Double>> s) ->{
            System.out.println("key:"+ s._1);
            System.out.println(s._2._1 + "," + s._2._2);
        });

        natureSorted.saveAsTextFile(outputPath + "/2");

        JavaPairRDD<String, Iterable<Tuple2<String, Double>>> groups = natureSorted.groupByKey();

        groups.collect().forEach((Tuple2<String, Iterable<Tuple2<String, Double>>> s) ->{
            System.out.println("key:"+ s._1);
            s._2.forEach((Tuple2<String, Double> item) ->{
                System.out.println(item._1 + "," + item._2);
            });
        });
        groups.saveAsTextFile(outputPath + "/3");

        JavaPairRDD<String, Iterable<Tuple2<String, Double>>> movingAverage = groups.mapValues((Iterable<Tuple2<String, Double>> timeSeries) ->{
           Queue<Tuple2<String, Double>> movingSet = new LinkedList<>();
           double sum  = 0;
           int size = timeWindow.getValue();
           List<Tuple2<String, Double>> movingAverageList = new ArrayList<>();
           for (Tuple2<String, Double> item : timeSeries){
               movingSet.add(item);
               sum += item._2;
               if (movingSet.size() > size){
                   sum -= movingSet.remove()._2;
               }
               double movingAverageValue = sum/movingSet.size();
               movingAverageList.add(new Tuple2<>(item._1, movingAverageValue));
           }
           return movingAverageList;
        });

        System.out.println("------------------------final result -------------------");
        movingAverage.collect().forEach((Tuple2<String, Iterable<Tuple2<String, Double>>> s) ->{
            System.out.println("key:"+ s._1);
            s._2.forEach((Tuple2<String, Double> item) ->{
                System.out.println(item._1 + "," + item._2);
            });
        });
        movingAverage.saveAsTextFile(outputPath + "/4");

    }

}
