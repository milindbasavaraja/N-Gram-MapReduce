package com.nyu.bigdata.Reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ExtraCreditReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        String biGram = null;
        double maxProbability = 0.0;

        for(Text value:values){
            String biGramDetails = value.toString();
            String biGramKey = biGramDetails.split("\t")[0];
            String  biGramProbability =  biGramDetails.split("\t")[1];

            double probability = Double.parseDouble(biGramProbability);
            if(probability > maxProbability){
                maxProbability = probability;
                biGram = biGramKey;
            }
        }

        context.write(new Text(biGram),new Text(String.valueOf(maxProbability)));
    }
}
