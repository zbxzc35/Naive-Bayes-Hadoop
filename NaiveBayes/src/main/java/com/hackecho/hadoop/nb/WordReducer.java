package com.hackecho.hadoop.nb;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> labelsCollection, Context context) throws InterruptedException,
            IOException {

        // Update the counter of the size of the vocabulary.
        context.getCounter(Main.NB_COUNTERS.VOCABULARY_SIZE).increment(1);

        // Loop through the labels.
        HashMap<String, Integer> counts = new HashMap<String, Integer>();
        for (Text label : labelsCollection) {
            String labelKey = label.toString();
            if (counts.containsKey(labelKey)) {
                counts.put(labelKey, counts.get(labelKey).intValue() + 1);
            } else {
                counts.put(labelKey, 1);
            }
        }
        StringBuilder output = new StringBuilder();
        for (String label : counts.keySet()) {
            int labelCount = counts.get(label).intValue();
            output.append(String.format("%s:%s ", label, labelCount));
        }

        // Write out the Map associated with the word.
        context.write(key, new Text(output.toString().trim()));

        // output : (word, 0:12 4:10) for this word, it has 12 neg labels, and
        // 10 pos labels
    }
}
