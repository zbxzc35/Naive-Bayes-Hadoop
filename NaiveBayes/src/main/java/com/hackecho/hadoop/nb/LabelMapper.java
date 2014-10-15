package com.hackecho.hadoop.nb;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hackecho.hadoop.nb.utils.Utils;

public class LabelMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
        String original = value.toString();
        String label = original.substring(0, 1);
        String text = Utils.preProcessTweet(original);
        String[] words = text.split(" ");

        context.write(new Text(label), new IntWritable(words.length));
    }
}
