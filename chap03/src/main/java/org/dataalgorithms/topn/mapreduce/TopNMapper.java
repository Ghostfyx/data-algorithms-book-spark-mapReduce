package org.dataalgorithms.topn.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-10 9:14
 */
public class TopNMapper extends Mapper<LongWritable,Text,NullWritable, Text> {

    /**
     * default top 10
     */
    private int N = 10;

    /**
     * SortedMap默认的排序是根据key值进行升序排序
     */
    private SortedMap<Double, String> topCats = new TreeMap<Double, String>();

    @Override
    public void map(LongWritable key, Text value, Context context){
        System.out.println("key:"+key);
        System.out.println("value:"+value.toString());
        String[] tokens = value.toString().split(",");
        // <cat_weight><,><cat_id><,><cat_name>
        double weight = Double.parseDouble(tokens[0]);
        String cat = value.toString();
        topCats.put(weight, cat);
        if (topCats.size() > this.N){
            topCats.remove(topCats.firstKey());
        }
    }

    /**
     * 驱动器函数，将设置传递到map，reduce函数
     *
     * @param context
     */
    @Override
    protected void setup(Context context){
        this.N = context.getConfiguration().getInt("N", 10);
    }

    /**
     * 每一个Mapper会接收一个cat分区，创建一个TopN散列表，将所有Mapper的结果输出到一个Reducer进行统一排序
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String cat : topCats.values()){
            context.write(NullWritable.get(), new Text(cat));
        }
    }

}
