package org.dataalgorithms.commonfriend.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-19
 */
public class CommonFriendsSpark {

    public static void main(String[] args){
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: Argument 1: input dir, Argument 2: output dir");
        }
        String inputDir = args[0];
        String outputDir = args[1];

        JavaSparkContext sc = new JavaSparkContext();
        JavaRDD<String> lines = sc.textFile(inputDir);

        JavaPairRDD<Tuple2<String, String>, Iterable<String>> pairs  = lines.flatMapToPair((String s) ->{
            String[] tokens = s.split(",");
            String person = tokens[0];
            String[] friends = tokens[1].split(" ");
            List<String> friendsList = Arrays.asList(friends);
            List<Tuple2<Tuple2<String, String> ,Iterable<String>>> result =
                    new ArrayList<Tuple2<Tuple2<String, String> ,Iterable<String>>>();
            for (String friend : friendsList){
                Tuple2<String, String> personTuple = buildSortedTuple(person, friend);
                result.add(new Tuple2<>(personTuple, friendsList));
            }
            return result.iterator();
        });
        pairs.saveAsTextFile(outputDir + "/1");

        JavaPairRDD<Tuple2<String, String>, Iterable<Iterable<String>>> grouped = pairs.groupByKey();

        JavaPairRDD<Tuple2<String, String>,Iterable<String>> commonFriends = grouped.mapValues((Iterable<Iterable<String>> s) -> {
            Map<String, Integer> countCommon = new HashMap<String, Integer>();
            int size = 0;
            for (Iterable<String> iter : s) {
                size++;
                List<String> list = iterableToList(iter);
                if ((list == null) || (list.isEmpty())) {
                    continue;
                }
                //
                for (String f : list) {
                    Integer count = countCommon.get(f);
                    if (count == null) {
                        countCommon.put(f, 1);
                    }
                    else {
                        countCommon.put(f, ++count);
                    }
                }
            }

            // if countCommon.Entry<f, count> ==  countCommon.Entry<f, s.size()>
            // then that is a common friend
            List<String> finalCommonFriends = new ArrayList<String>();
            for (Map.Entry<String, Integer> entry : countCommon.entrySet()){
                if (entry.getValue() == size) {
                    finalCommonFriends.add(entry.getKey());
                }
            }
            return finalCommonFriends;
        });

        commonFriends.saveAsTextFile(outputDir + "/2");
    }


    public static Tuple2<String, String> buildSortedTuple(String person, String friend){
        long p = Long.parseLong(person);
        long f = Long.parseLong(friend);
        if (p < f) {
            return new Tuple2<>(person, friend);
        } else {
            return new Tuple2<>(friend, person);
        }
    }

    public static List<String> iterableToList(Iterable<String> iterable) {
        List<String> list = new ArrayList<String>();
        for (String item : iterable) {
            list.add(item);
        }
        return list;
    }
}
