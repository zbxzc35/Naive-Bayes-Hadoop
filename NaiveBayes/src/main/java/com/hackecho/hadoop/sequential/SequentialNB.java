package com.hackecho.hadoop.sequential;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Date;

import weka.classifiers.bayes.NaiveBayes;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.StringToWordVector;

import com.hackecho.hadoop.nb.utils.*;

public class SequentialNB {

    public static void main(String[] args) {
        long start = new Date().getTime();
        String inputFileName = "training.csv";
        String outputFileName = "training.arff";

        Utils.convertToARFF(inputFileName, outputFileName);

        long end = new Date().getTime();
        System.out.println("Convertion took " + (end - start) + " milliseconds");

        NaiveBayes nb = new NaiveBayes();
        StringToWordVector stv = new StringToWordVector();
        Instances data = null;
        Instances filteredData = null;
        String[] options = { "-R", "first-last", "-W", "1000", "-prune-rate", "-1.0", "-N", "0", "-stemmer",
                "weka.core.stemmers.NullStemmer", "-M", "1", "-tokenizer", "weka.core.tokenizers.WordTokenizer",
                "-delimiters", " \r\n\t.,;:\'\''()?!" };

        try {
            // Tokenizer
            InputStream modelIn = new FileInputStream("src/resources/en-token.bin");
            if (modelIn != null) {
                modelIn.close();
            }
            // Training
            System.out.println("===> Start training...");
            FileReader fr = new FileReader(outputFileName);
            BufferedReader br = new BufferedReader(fr);
            data = new Instances(br);
            data.setClassIndex(data.numAttributes() - 1);
            stv.setOptions(options);
            stv.setInputFormat(data);
            filteredData = Filter.useFilter(data, stv);
            nb.buildClassifier(filteredData);
            System.out.println("===> Training is done!");
            fr.close();
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        end = new Date().getTime();
        System.out.println("All the training took " + (end - start) + " milliseconds (" + (end - start) / 1000 / 60.0 + " minutes)");
    }
}
