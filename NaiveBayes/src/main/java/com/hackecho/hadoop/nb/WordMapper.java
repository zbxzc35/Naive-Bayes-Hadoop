package com.hackecho.hadoop.nb;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hackecho.hadoop.nb.utils.Utils;

public class WordMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
        String original = value.toString();
        String label = original.substring(0, 1);
        String text = Utils.preProcessTweet(original);
        String[] words = text.split(" ");

        for (String word : words) {
            context.write(new Text(word), new Text(label));
        }
    }

    // output: (word, label)

}
