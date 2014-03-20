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

import java.util.Arrays;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.*;
//import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.math.linear.RealVector;
import org.apache.commons.math.linear.ArrayRealVector;


public class testCache extends Configured implements Tool{

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text vecID = new Text();
        private Text vec = new Text();
        String vecSplitter = ",";

        
        private Path[] localFiles;
        //FileInputStream fis = new FileInputStream();
        //BufferedInputStream bis = new BufferedInputStream;
        
        
    
         @Override
        protected void setup(Context context) throws IOException,InterruptedException {
        super.setup(context);
         //URI[] uris = DistributedCache.getCacheFiles(context.getConfiguration());
        localFiles = context.getLocalCacheFiles();
    // TODO
    } 

        
        
        
        
        
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException{
            
            Configuration conf = context.getConfiguration();
            int NUMFEATURES = Integer.parseInt(conf.get("numberFeatures"));
            int NUMCENTROIDS = Integer.parseInt(conf.get("numberCentroids"));

            
            
            
            
            
            String line = value.toString();
            String[] vecArray = line.split(vecSplitter);
            vecID.set("hello");
            
            
            
                //File file = new File(localFiles[0].toString());
                //fis = new FileInputStream(file);
                //bis = new BufferedInputStream(fis);
            //BufferedReader d = new BufferedReader(new InputStreamReader(bis));
           
            ArrayRealVector vecParse = lineToVector(line, NUMFEATURES);
            vec.set(localFiles[0].toString()+"test"+vecParse.toString()); 
            
            context.write(vecID, vec);
        }
         
        
            private ArrayRealVector lineToVector(String line, int NUMFEATURES ){
            String[] vecArray = line.split(vecSplitter);
            
            ArrayRealVector dd = new ArrayRealVector();
            for(int j=0 ; j < NUMFEATURES; j++){
            dd.append(Double.parseDouble(vecArray[j+1]));   
            //dd[j]=Double.parseDouble(vecArray[j+1]);    
           }
           return dd;  
        }

        
        
        
        
    }
   
    public int run(String[] args) throws Exception{
        Configuration conf = new Configuration();
        
        Job job = new Job(conf, "testCache");
        job.addCacheFile(new URI("hdfs://zphdc1n1:8020/user/clakhani/anchorgraph/centroids.txt"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);        
        job.setMapperClass(Map.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJarByClass(testCache.class);
        job.submit();
        int rc = (job.waitForCompletion(true)) ? 1 : 0;
	return rc;
    }
      public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new testCache(), args);
    System.exit(res);
      }
}



