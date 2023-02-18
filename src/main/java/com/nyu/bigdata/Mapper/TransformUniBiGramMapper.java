package com.nyu.bigdata.Mapper;

import com.nyu.bigdata.model.StringPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This class is responsible to create composite key as follows:
 * If word is UniGram ----> Composite key: (UniGram,UniGram) .
 * If word is BiGram ----> Composite key: (UniGram,BiGram and BiGram count) .
 */
public class TransformUniBiGramMapper extends Mapper<LongWritable, Text, StringPair, Text> {


    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
        String[] words = inputValue.toString().split("\t");
        String word = words[0];
        String wordCount = words[1];
        String[] keys = word.split(" ");

        String firstString = keys[0];

        // check if input line is UniGram/BiGram.
        if (keys.length == 1) {

            context.write(new StringPair(firstString, firstString), new Text(wordCount));
        } else {
            String secondString = keys[0] + " " + keys[1];
            context.write(new StringPair(firstString, secondString), inputValue);
        }

    }

}
