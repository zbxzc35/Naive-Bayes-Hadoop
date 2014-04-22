package com.hackecho.hadoop.nb;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TrainMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text key_l_t = new Text();
	private Text key_l = new Text();
	private Text key_t = new Text();
	private final static IntWritable one = new IntWritable(1);
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String label = String.valueOf(value.toString().toCharArray()[0]);
		String sample = value.toString().substring(1, value.getLength());
		StringTokenizer wordList = new StringTokenizer(sample);
		while (wordList.hasMoreTokens()) {
			String tempWord = wordList.nextToken();
			// key:    1 | label | token
			// value:  1
			key_l_t.set("1"+label+"|"+tempWord);
			// key:    2 | label
			// value:  1
			key_l.set("2"+label);
			// key:    3 | token
			// value:  1
			key_t.set("3"+tempWord);
			context.write(key_l_t, one);
			context.write(key_l, one);
			context.write(key_t, one);
		}
	}
}
