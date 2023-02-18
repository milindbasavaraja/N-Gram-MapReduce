package com.nyu.bigdata.Reducer;

import com.nyu.bigdata.NGramMain;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This Reducer is responsible to find Total Uni-Gram and Bi-Gram count.
 * It also calculates Uni-Gram and Bi-Gram count
 * Input Format ---> Key: [{Values}] => Text: [{LongWritable}]
 * Eg: zulu:[{1,1,1,1}]
 * Output Format ---> (Key,Value) => (Text,LongWritable)
 * Eg: zulus, 4
 */
public class UniBiGramCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private final Logger logger = LoggerFactory.getLogger(UniBiGramCountReducer.class);

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long uniGramCount = 0;
        long biGramCount = 0;
        String[] keyString = key.toString().split(" ");
        // If keyString is a UniGram then the length of array would be 1
        if (keyString.length == 1){
            for(LongWritable value:values){
                uniGramCount = uniGramCount + value.get();
            }
            //Increment TOTAL_UNI_GRAM_COUNT
            context.getCounter(NGramMain.CustomCounter.TOTAL_UNI_GRAM_COUNT).increment(uniGramCount);

            context.write(key,new LongWritable(uniGramCount));
        }
        //If keyString is a BiGram then the length of array should be 2
        else if (keyString.length == 2){
            for(LongWritable value:values){
                biGramCount = biGramCount + value.get();
            }

            //Increment TOTAL_BI_GRAM_COUNT
            context.getCounter(NGramMain.CustomCounter.TOTAL_BI_GRAM_COUNT).increment(biGramCount);
            context.write(key,new LongWritable(biGramCount));
        }

    }
}
