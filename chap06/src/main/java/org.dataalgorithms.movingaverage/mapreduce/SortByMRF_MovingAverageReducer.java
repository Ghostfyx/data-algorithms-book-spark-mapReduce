package org.dataalgorithms.movingaverage.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.dataalgorithms.movingaverage.pojo.SimpleMovingAverage;
import org.dataalgorithms.movingaverage.util.DateUtil;

import java.io.IOException;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-15
 */
public class SortByMRF_MovingAverageReducer extends Reducer<CompositeKey, TimeSeriesData, Text, Text> {

    int windowSize = 3;

    @Override
    public void reduce(CompositeKey key,
                       Iterable<TimeSeriesData> values,
                       Context context) throws IOException, InterruptedException {
        Text outputKey = new Text();
        Text outputValue = new Text();
        SimpleMovingAverage ma = new SimpleMovingAverage(this.windowSize);
        for (TimeSeriesData timeSeriesData :values){
            ma.addNewNumber(timeSeriesData.getValue());
            double movingAverage = ma.getMovingAverage();
            long timestamp = timeSeriesData.getTimeStamp();
            String dateAsString = DateUtil.getDateAsString(timestamp);
            outputValue.set(dateAsString + "," + movingAverage);
            outputKey.set(key.getName());
            context.write(outputKey, outputValue);
        }
    }

    /**
     * will be run only once get parameters from Hadoop's configuration
     */
    @Override
    protected void setup(Context context){
        this.windowSize = context.getConfiguration().getInt("windowSize", 5);
    }

}
