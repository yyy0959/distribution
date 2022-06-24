package DSPPCode.mapreduce.logistic_regression.question;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public abstract class LogisticReducer extends Reducer<IntWritable, DoubleWritable, Text, DoubleWritable> {

  /**
   * TODO 请完成该方法
   * <p>
   * 输出:
   * <p>
   * 根据题目要求输出对应的键值对
   */
  @Override
  public abstract void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException;

}
