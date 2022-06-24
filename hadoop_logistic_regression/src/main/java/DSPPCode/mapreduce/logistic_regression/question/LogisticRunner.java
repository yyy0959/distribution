package DSPPCode.mapreduce.logistic_regression.question;

import DSPPCode.mapreduce.logistic_regression.impl.LogisticMapperImpl;
import DSPPCode.mapreduce.logistic_regression.impl.LogisticReducerImpl;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.*;
import java.net.URI;

public class LogisticRunner extends Configured implements Tool {
  public static int iteration = 0;
  public static double w1 = 1.0;
  public static double w2 = 1.0;

  @Override
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), getClass().getSimpleName());
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]+iteration));

    job.setMapperClass(LogisticMapperImpl.class);
    job.setReducerClass(LogisticReducerImpl.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    if(iteration > 0){
      job.addCacheFile(new URI(args[1]+(iteration-1)));
    }

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public int main(String[] args) throws Exception {
    int exitcode = 0;

    for(int i=0; i<87; i++){
      exitcode = ToolRunner.run(new LogisticRunner(), args);
      iteration++;
    }

    return 0;
  }

}

