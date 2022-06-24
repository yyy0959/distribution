package DSPPCode.mapreduce.logistic_regression.question;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public abstract class LogisticMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {

  @Override
  public abstract void map(Object key, Text value, Context context)
      throws IOException, InterruptedException;

}
