package com.pat.tugas2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MeanAuthor {
    
    public static void main(String[] args) throws Exception {
        new MeanAuthor().run(args);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set(XmlInputFormat.START_TAG_KEY, "article|inproceedings|www|proceedings|book|incollection|phdthesis|mastersthesis");
        conf.set("io.serializations",
                "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        
        Job meanJob = Job.getInstance(conf, "mean-search");
        meanJob.setJarByClass(MeanAuthor.class);
        
        meanJob.setOutputKeyClass(Text.class);
        meanJob.setOutputValueClass(DoubleWritable.class);
        
        meanJob.setMapperClass(AuthorMapper.class);
        meanJob.setReducerClass(AuthorReducer.class);
        
        meanJob.setInputFormatClass(XmlInputFormat.class);
        meanJob.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(meanJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(meanJob, new Path(args[1]));

        try {
            meanJob.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException ex) {
            System.out.println(ex);
        }
        
        return 0;
    }
}

