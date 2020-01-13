package org.dataalgorithms.chap02.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-09
 */
public class SecondarySortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Secondary Sort Use CompositeKey In MR");

        job.setJarByClass(SecondarySortDriver.class);

        // set mapper and reducer
        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);

        // 定义了自然键和组合键的bean
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(NaturalValue.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //定义自然键的分区
        job.setPartitionerClass(NaturalKeyPartitioner.class);
        //定义分区内自然键的排序
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        //定义组合键的排序
        job.setSortComparatorClass(CompositeKeyComparator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);
        System.out.println(status);
    }

}
