package DSPPCode.mapreduce.logistic_regression.impl;

import DSPPCode.mapreduce.logistic_regression.question.LogisticMapper;
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
import org.python.antlr.op.In;
import java.net.URI;
import  java.io.*;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LogisticMapperImpl extends LogisticMapper{

  DoubleWritable ret = new DoubleWritable();
  IntWritable k = new IntWritable();
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
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] line = value.toString().split(" ");
    double y = Double.parseDouble(line[0]);
    double x1 = Double.parseDouble(line[1]);
    double x2 = Double.parseDouble(line[2]);

    double grad1 = (2)*(y-w1*x1-w2*x2)*x1;
    double grad2 = (2)*(y-w1*x1-w2*x2)*x2;

    k.set(1);
    ret.set(grad1);
    context.write(k, ret);

    k.set(2);
    ret.set(grad2);
    context.write(k, ret);
  }
}
