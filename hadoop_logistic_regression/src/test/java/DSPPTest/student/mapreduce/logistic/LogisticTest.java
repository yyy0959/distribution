package DSPPTest.student.mapreduce.logistic;


import DSPPCode.mapreduce.logistic_regression.question.LogisticRunner;
import DSPPTest.student.TestTemplate;
import DSPPTest.util.Parser.KVParser;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyKV;

public class LogisticTest extends TestTemplate {

  @Test
  public void test() throws Exception {
    // 设置路径
    String inputPath = root + "/mapreduce/logistic_regression/input";
    String outputPath = "C:\\Users\\yuzhiyuan\\Desktop\\Distributed-Computing-Systems\\hadoop_logistic_regression\\src\\test\\resources\\student\\mapreduce\\logistic_regression\\answer\\answer";
    //String outputPath = root + "/mapreduce/logistic_regression/answer/answer";
    String outputFile = outputPath + "/part-r-00000";
    String answerFile = root + "/mapreduce/logistic/answer";

    // 删除旧输出
    deleteFolder(outputPath);

    // 执行
    String[] args = {inputPath, outputPath};

    new LogisticRunner().main(args);

    // 检验结果
    // verifyKV(readFile2String(outputFile), readFile2String(answerFile), new KVParser("\t"));
    // System.out.println("恭喜通过~");
    // System.exit(exitCode);
  }

}
