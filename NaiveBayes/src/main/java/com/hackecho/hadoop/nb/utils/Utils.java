package com.hackecho.hadoop.nb.utils;

public class Utils {

    /**
     * Pre-process the tweet
     * 
     * @author: Zhaoyu
     * @param tweet
     * @return
     */
    public static String preProcessTweet(String tweet) {
        return tweet.replaceAll("((www\\.[\\s]+)|(https?://[^\\s]+))", "").replaceAll("@[^\\s]+", "")
                .replaceAll("#([^\\s]+)", "\1").replace('"', ' ').replaceAll("[0-9][a-zA-Z0-9]*", "").replace(',', ' ')
                .replace('.', ' ').replace('!', ' ').replace('?', ' ').replace(':', ' ').replace('*', ' ')
                .replace('\'', ' ').replaceAll("&quot;", " ");
    }
}
