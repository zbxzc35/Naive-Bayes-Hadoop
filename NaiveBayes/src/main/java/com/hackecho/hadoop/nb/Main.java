package com.hackecho.hadoop.nb;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

    public static final double ALPHA = 1.0;

    static enum NB_COUNTERS {
        TOTAL_DOCS, VOCABULARY_SIZE, UNIQUE_LABELS
    }

    public static final String TOTAL_DOCS = "com.hackecho.hadoop.nb.total_docs";
    public static final String VOCABULARY_SIZE = "com.hackecho.hadoop.nb.vocabulary_size";
    public static final String UNIQUE_LABELS = "com.hackecho.hadoop.nb.unique_labels";

    public static void delete(Configuration conf, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Configuration classifyConf = new Configuration();

        Path traindata = new Path(conf.get("train"));
        Path testdata = new Path(conf.get("test"));
        Path output = new Path(conf.get("output"));
        //int numReducers = conf.getInt("reducers", 5);
        Path distCache = new Path(output.getParent(), "cache");
        Path model = new Path(output.getParent(), "model");
        Path joined = new Path(output.getParent(), "joined");

        // Job 1a: Extract information on each word.
        Main.delete(conf, model);
        Job trainWordJob = new Job(conf, "nb-wordtrain");
        trainWordJob.setJarByClass(Main.class);
        //trainWordJob.setNumReduceTasks(numReducers);
        trainWordJob.setMapperClass(WordMapper.class);
        trainWordJob.setReducerClass(WordReducer.class);

        trainWordJob.setInputFormatClass(TextInputFormat.class);
        trainWordJob.setOutputFormatClass(TextOutputFormat.class);

        trainWordJob.setMapOutputKeyClass(Text.class);
        trainWordJob.setMapOutputValueClass(Text.class);
        trainWordJob.setOutputKeyClass(Text.class);
        trainWordJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(trainWordJob, traindata);
        FileOutputFormat.setOutputPath(trainWordJob, model);

        if (!trainWordJob.waitForCompletion(true)) {
            System.err.println("ERROR: Word training failed!");
            return 1;
        }

        classifyConf.setLong(Main.VOCABULARY_SIZE,
                trainWordJob.getCounters().findCounter(Main.NB_COUNTERS.VOCABULARY_SIZE).getValue());

        // Job 1b: Tabulate label-based statistics.
        Main.delete(conf, distCache);
        Job trainLabelJob = new Job(conf, "nb-labeltrain");
        trainLabelJob.setJarByClass(Main.class);
        //trainLabelJob.setNumReduceTasks(numReducers);
        trainLabelJob.setMapperClass(LabelMapper.class);
        trainLabelJob.setReducerClass(LabelReducer.class);

        trainLabelJob.setInputFormatClass(TextInputFormat.class);
        trainLabelJob.setOutputFormatClass(TextOutputFormat.class);

        trainLabelJob.setMapOutputKeyClass(Text.class);
        trainLabelJob.setMapOutputValueClass(IntWritable.class);
        trainLabelJob.setOutputKeyClass(Text.class);
        trainLabelJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(trainLabelJob, traindata);
        FileOutputFormat.setOutputPath(trainLabelJob, distCache);

        if (!trainLabelJob.waitForCompletion(true)) {
            System.err.println("ERROR: Label training failed!");
            return 1;
        }
        
        /**
        classifyConf.setLong(Main.UNIQUE_LABELS, trainLabelJob.getCounters()
                .findCounter(Main.NB_COUNTERS.UNIQUE_LABELS).getValue());
        classifyConf.setLong(Main.TOTAL_DOCS, trainLabelJob.getCounters().findCounter(Main.NB_COUNTERS.TOTAL_DOCS)
                .getValue());

        // Job 2: Reduce-side join the test dataset with the model.
        Main.delete(conf, joined);
        Job joinJob = new Job(conf, "nb-testprep");
        joinJob.setJarByClass(Main.class);
        joinJob.setNumReduceTasks(numReducers);
        MultipleInputs.addInputPath(joinJob, model, KeyValueTextInputFormat.class, ModelMapper.class);
        MultipleInputs.addInputPath(joinJob, testdata, TextInputFormat.class, JoinMapper.class);
        joinJob.setReducerClass(JoinReducer.class);

        joinJob.setOutputFormatClass(TextOutputFormat.class);

        joinJob.setMapOutputKeyClass(Text.class);
        joinJob.setMapOutputValueClass(Text.class);
        joinJob.setOutputKeyClass(Text.class);
        joinJob.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(joinJob, joined);

        if (!joinJob.waitForCompletion(true)) {
            System.err.println("ERROR: Joining failed!");
            return 1;
        }
        **/
        
        /**
        // Job 3: Classification!
        Main.delete(classifyConf, output);

        // Add to the Distributed Cache.
        FileSystem fs = distCache.getFileSystem(classifyConf);
        Path pathPattern = new Path(distCache, "part-r-[0-9]*");
        FileStatus[] list = fs.globStatus(pathPattern);
        for (FileStatus status : list) {
            DistributedCache.addCacheFile(status.getPath().toUri(), classifyConf);
        }
        Job classify = new Job(classifyConf, "nb-classify");
        classify.setJarByClass(Main.class);
        classify.setNumReduceTasks(numReducers);

        classify.setMapperClass(ClassifyMapper.class);
        classify.setReducerClass(ClassifyReducer.class);

        classify.setInputFormatClass(KeyValueTextInputFormat.class);
        classify.setOutputFormatClass(TextOutputFormat.class);

        classify.setMapOutputKeyClass(LongWritable.class);
        classify.setMapOutputValueClass(Text.class);
        classify.setOutputKeyClass(LongWritable.class);
        classify.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(classify, joined);
        FileOutputFormat.setOutputPath(classify, output);

        if (!classify.waitForCompletion(true)) {
            System.err.println("ERROR: Classification failed!");
            return 1;
        }

        // Last job: manually read through the output file and
        // sort the list of classification probabilities.

        int correct = 0;
        int total = 0;
        pathPattern = new Path(output, "part-r-[0-9]*");
        FileStatus[] results = fs.globStatus(pathPattern);
        for (FileStatus result : results) {
            FSDataInputStream input = fs.open(result.getPath());
            BufferedReader in = new BufferedReader(new InputStreamReader(input));
            String line;
            while ((line = in.readLine()) != null) {
                String[] pieces = line.split("\t");
                correct += (Integer.parseInt(pieces[1]) == 1 ? 1 : 0);
                total++;
            }
            IOUtils.closeStream(in);
        }

        System.out.println(String.format("%s/%s, accuracy %.2f", correct, total,
                ((double) correct / (double) total) * 100.0));
        **/
        System.out.println("Done!");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        long start = new Date().getTime();
        int exitCode = ToolRunner.run(new Main(), args);
        long end = new Date().getTime();
        System.out.println("Job took "+ (end - start) + " milliseconds");
        System.exit(exitCode);
    }
}
