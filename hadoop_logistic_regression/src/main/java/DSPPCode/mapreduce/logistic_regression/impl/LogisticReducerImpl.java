package DSPPCode.mapreduce.logistic_regression.impl;

import DSPPCode.mapreduce.logistic_regression.question.LogisticReducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import DSPPCode.mapreduce.logistic_regression.question.LogisticRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.net.URI;
import  java.io.*;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LogisticReducerImpl extends LogisticReducer{
  DoubleWritable ret = new DoubleWritable();
  Text k = new Text();
  public static double w1 = 1.0;
  public static double w2 = 1.0;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    URI[] cacheFiles = context.getCacheFiles();
    if (cacheFiles != null && cacheFiles.length > 0)
    {
      try {
        String a = "";
        String b = "";
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path getFilePath = new Path(cacheFiles[0].toString());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
        a = reader.readLine();
        w1 = Double.parseDouble(a.split("\t")[1]);
        b = reader.readLine();
        w2 = Double.parseDouble(b.split("\t")[1]);
      }
      catch (Exception e)
      {
        System.out.println("Unable to read the File");
        System.exit(1);
      }
    }
  }

  @Override
  public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {

    // System.out.println(key.toString());
    double total_grad = 0;
    for (DoubleWritable value : values) {
      total_grad += value.get();
    }
    if(key.get()==1){
      LogisticRunner.w1 += total_grad/100000000;
      k.set("w1");
      ret.set(LogisticRunner.w1 + total_grad/100000000);
      context.write(k, ret);
    }
    if(key.get()==2){
      LogisticRunner.w2 += total_grad/100000000;
      k.set("w2");
      ret.set(LogisticRunner.w2 + total_grad/100000000);
      context.write(k, ret);
    }

    // if(Integer.parseInt(key.toString())!=1){
    //   System.out.println("124124");
    // }
  }
}