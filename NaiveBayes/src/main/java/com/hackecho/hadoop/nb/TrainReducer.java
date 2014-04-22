package com.hackecho.hadoop.nb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrainReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	/*
	 * TODO: ADD P(w|c)
	 */
	Map<String, IntWritable> perLabelCount = new HashMap<String, IntWritable>();
	private final static IntWritable one = new IntWritable(1);
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int totalWordCount = 0;
		int totalLabelCount = 0;
		
		Iterator<IntWritable> it = values.iterator();
		while (it.hasNext()) {
			int tempValue = it.next().get();
			// get type of key
			String[] keyToken =  key.toString().split("|");
			if (keyToken[0] == "1") {
				// label | token
			} else if (keyToken[0] == "2") {
				// label
				IntWritable number = perLabelCount.get(keyToken[1]);
				if (number != null) {
					perLabelCount.put(keyToken[1].toString(), new IntWritable(number.get()+1));
				} else {
					perLabelCount.put(keyToken[1].toString(), one);
				}
				totalLabelCount += it.next().get();
			} else {
				// token
				totalWordCount += it.next().get();
			}
		}
		for (Map.Entry<String, IntWritable> entry : perLabelCount.entrySet()) {
			// P(c)
		    String key_l = entry.getKey();
		    int value = entry.getValue().get();
		    context.write(new Text(key_l), new IntWritable(value/totalLabelCount));
		}
	}
}
