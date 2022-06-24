package DSPPCode.spark.logistic_regression.impl;

import DSPPCode.spark.logistic_regression.question.DataPoint;
import DSPPCode.spark.logistic_regression.question.IterationStep;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class IterationStepImpl extends IterationStep {

  //TODO:终止条件，当新的权重和旧的权重平方距离(欧式距离的平方)小小于阈值（0.01）时迭代终止
  public boolean termination(double[] old, double[] newWeight) {
    double gap = 0.0;
    for (int i = 0; i < old.length; i++) {
      gap += (newWeight[i] - old[i]) * (newWeight[i] - old[i]);
    }
    return gap <= THRESHOLD;
  }

  //TODO：利用此类中给的工具类，根据梯度下降法求解梯度
  public double[] runStep(JavaRDD<DataPoint> points, double[] weights) {
    double[] new_weight = new double[weights.length];
    ComputeGradient computeGradient = new ComputeGradient(weights);
    VectorSum vectorSum = new VectorSum();
    double[] gradient = points.map((point)->(computeGradient.call(point))).reduce((x,y)->(vectorSum.call(x,y)));
    for (int i = 0; i < new_weight.length; i++) {
      new_weight[i] = weights[i] - STEP * gradient[i] / 1000000;
    }
    return new_weight;
  }

  //TODO：实现向量求和
  public static class VectorSum implements Function2<double[], double[], double[]> {
    @Override
    public double[] call(double[] a, double[] b) throws Exception {
      if(a.length != b.length) return null;
      double[] ret = new double[a.length];
      for (int i = 0; i < a.length; i++) {
        ret[i] = a[i] + b[i];
      }
      return ret;
    }
  }

  //TODO：根据readme所给公式求梯度
  public class ComputeGradient implements Function<DataPoint, double[]> {

    public final double[] weights;

    public ComputeGradient(double[] weights) { this.weights = weights; }

    @Override
    public double[] call(DataPoint dataPoint) throws Exception {
      double[] grad = new double[weights.length];
      double w_T_x = 0.0;
      for (int i = 0; i < weights.length; i++) {
        w_T_x += dataPoint.x[i] * weights[i];
      }
      for (int i = 0; i < weights.length; i++) {
        grad[i] = dataPoint.x[i] * (w_T_x - dataPoint.y);
      }
      return grad;
    }
  }

}
