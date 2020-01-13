package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @description: 自然键，自定义输入类需要实现比较器，为了在map之后进行排序
 * @author: fanyeuxiang
 * @createDate: 2020-01-08 10:51
 */
public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {

    private final Text yearMonth = new Text();
    private final Text day = new Text();
    private final IntWritable temperature = new IntWritable();

    public DateTemperaturePair() {
    }

    public DateTemperaturePair(String yearMonth, String day, int temperature) {
        this.yearMonth.set(yearMonth);
        this.day.set(day);
        this.temperature.set(temperature);
    }

    @Override
    public int compareTo(DateTemperaturePair o) {
        int compareValue = this.yearMonth.compareTo(o.getYearMonth());
        if (compareValue == 0){
            compareValue = this.temperature.compareTo(o.getTemperature());
        }
        return compareValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        yearMonth.write(dataOutput);
        day.write(dataOutput);
        temperature.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        yearMonth.readFields(dataInput);
        day.readFields(dataInput);
        temperature.readFields(dataInput);
    }

    public Text getYearMonth() {
        return yearMonth;
    }

    public Text getDay() {
        return day;
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    public Text getYearMonthDay() {
        return new Text(yearMonth.toString()+day.toString());
    }

    public void setYearMonth(String yearMonthAsString) {
        yearMonth.set(yearMonthAsString);
    }

    public void setDay(String dayAsString) {
        day.set(dayAsString);
    }

    public void setTemperature(int temp) {
        temperature.set(temp);
    }
}
