package com.nyu.bigdata.Reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class BiGramLMProbabilityReducer extends Reducer<Text,Text,Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        for (Text value: values){
            String[] countDetails = value.toString().split("\t");
            String[] biGramDetails = countDetails[0].split(" ");
            String[] uniGramDetails = countDetails[1].split(" ");

            long biGramCount = Long.parseLong(biGramDetails[0]);
            long totalBiGramCount = Long.parseLong(biGramDetails[1]);

            long uniGramCount = Long.parseLong(uniGramDetails[0]);
            long totalUniGramCount = Long.parseLong(uniGramDetails[1]);

            DecimalFormat dec = new DecimalFormat("#0.00");
            double probabilityW1W2 = (double)biGramCount/totalBiGramCount;
            double probabilityW1 = (double)uniGramCount/totalUniGramCount;
            double finalProbability = probabilityW1W2/probabilityW1;

            context.write(key,new DoubleWritable(finalProbability));

        }
    }
}
