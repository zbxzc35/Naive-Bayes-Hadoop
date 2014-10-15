package com.hackecho.hadoop.nb;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// input from mapper: (label, number of words in that sentence)  => (0, 5)

public class LabelReducer extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws InterruptedException,
            IOException {

        // get the number of unique label.
        context.getCounter(Main.NB_COUNTERS.UNIQUE_LABELS).increment(1);

        long pY = 0;
        long pYW = 0;
        for (IntWritable value : values) {
            // Increment the global counter total docs.
            context.getCounter(Main.NB_COUNTERS.TOTAL_DOCS).increment(1);

            // Increment the number of documents with this label.
            pY++;

            // Increment the number of words under this label. (including
            // duplicates)
            pYW += value.get();
        }

        // <label Y> {<# of documents with label Y>:<# of words under label Y>}
        context.write(key, new Text(String.format("%s:%s", pY, pYW)));
        // (0, 12:500)
        // (neg,docs:words)
    }
}
