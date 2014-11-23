package com.pat.tugas2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AuthorReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
	private DoubleWritable result = new DoubleWritable();
        @Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
            Context context) throws IOException, InterruptedException {
		double sum =0;
		double n=0;
		for(DoubleWritable val : values){
			sum += val.get();
			n+=1;
		}
		result.set(sum/n);
		context.write(key, result);
	}
}