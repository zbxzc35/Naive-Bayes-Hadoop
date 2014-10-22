package com.hackecho.hadoop.nb.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

// convert the training.csv file to an arff file

public class ToARFF {

    public static void main(String[] args) {
        long start = new Date().getTime();
        String inputFileName = "training.csv";
        String outputFileName = "training.arff";
        String line = null;
        try {
            FileReader fileReader = new FileReader(inputFileName);
            FileWriter fileWriter = new FileWriter(outputFileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write("@RELATION tweet\n\n@ATTRIBUTE text string\n@ATTRIBUTE class1 {pos,neg}\n\n@DATA\n");
            bufferedWriter.flush();
            while ((line = bufferedReader.readLine()) != null) {
                String temp = "";
                if (line.substring(0, 1).equals("0")) {
                    temp = "pos";
                } else {
                    temp = "neg";
                }
                line = line.replaceAll("((www\\.[\\s]+)|(https?://[^\\s]+))", "").replaceAll("@[^\\s]+", "")
                        .replaceAll("#([^\\s]+)", "\1").replace('"', ' ').replaceAll("[0-9][a-zA-Z0-9]*", "")
                        .replace(',', ' ').replace('.', ' ').replace('!', ' ').replace('?', ' ').replace(':', ' ')
                        .replace('*', ' ').replace('\'', ' ').replace('-', ' ').replaceAll("&quot;", " ").trim();
                String str = "\"" + line + "\"," + temp + "\n";
                bufferedWriter.write(str);
                bufferedWriter.flush();
            }
            bufferedReader.close();
            bufferedWriter.close();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        System.out.println("Done!");
        long end = new Date().getTime();
        System.out.println("The convertion took " + (end - start) + " milliseconds");
    }
}
