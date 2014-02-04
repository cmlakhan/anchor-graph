/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.lakhani.anchorgraph;

/**
 *
 * @author cmlakhan
 */
  import java.io.IOException;
  import java.util.*;
           
  import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
  import org.apache.hadoop.io.*;
  import org.apache.hadoop.mapreduce.*;
  import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
  import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
  import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
  import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class anchorgraph extends Configured implements Tool{
    
    public static class Map extends Mapper<LongWritable, Text, Text, VectorWritable> {
        private final static VectorWritable vec = new VectorWritable();
        private Text vecID = new Text();
        String vecSplitter = ",";
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] vecArray = line.split(vecSplitter);
            vecID.set(vecArray[0]);
            int vecLength = vecArray.length;
            
           double dd[] = new double[vecLength-1];
           
           for(int j=1 ; j < vecLength; j++){
               dd[j-1]=Double.parseDouble(vecArray[j]);    
           }
           
           DenseVector actualVec = new DenseVector(dd);
           vec.set(actualVec);
           context.write(vecID, vec);
        }
    }



    public int run(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "anchorgraph");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VectorWritable.class);
        
        job.setMapperClass(Map.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJarByClass(anchorgraph.class);
        job.submit();
        
        int rc = (job.waitForCompletion(true)) ? 1 : 0;
	return rc;

        
    }

    
      public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new anchorgraph(), args);
    System.exit(res);
  }

    
}
