package com.nyu.bigdata.Reducer;

import com.nyu.bigdata.NGramMain;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class UniGramCountReducer extends Reducer<Text, Text, Text, Text> {

    private final Logger logger = LoggerFactory.getLogger(UniGramCountReducer.class);

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //logger.info("The Uni-Gram Count Reducer started............");
        long uniGramCount = 0;
        long biGramCount = 0;
        String biGramWord = null;
        String uniGramWord = null;

        for(Text value: values){
            String[] customValueArray = value.toString().split("\t");
            String biGramWithCountString = customValueArray[0];
            String uniGramWithCountString = customValueArray[1];

            String[]  biGramWordDetailArray = biGramWithCountString.split(" ");
            if(!biGramWordDetailArray[0].equalsIgnoreCase("Default_Value")){
                long biGramWordCount = Long.parseLong(biGramWordDetailArray[2]);
                biGramWord = biGramWordDetailArray[0] + " "+biGramWordDetailArray[1];
                biGramCount = biGramCount + biGramWordCount;
            }else{
                biGramWord = "Default_Value";
            }
            String[] uniGramWordDetailArray = uniGramWithCountString.split(" ");
            long uniGramWordCount = Long.parseLong(uniGramWordDetailArray[1]);
            uniGramWord = uniGramWordDetailArray[0];
            uniGramCount = uniGramCount + uniGramWordCount;


        }
        context.getCounter(NGramMain.CustomCounter.TOTAL_UNI_GRAM_COUNT).increment(uniGramCount);
        context.getCounter(NGramMain.CustomCounter.TOTAL_BI_GRAM_COUNT).increment(biGramCount);


       /* logger.info("The unigram is: {} and its count is: {}",uniGramWord,uniGramCount);
        logger.info("The bigram is: {} and its count is: {}",biGramWord,biGramCount);*/

        String finalValueString = biGramWord + " "+biGramCount + "\t"+uniGramWord + " "+uniGramCount;

        context.write(new Text(biGramWord),new Text(finalValueString));
        /*if (word.trim().length() != 0) {
            for (LongWritable value : values) {
                count = count + value.get();
            }
            context.getCounter(NGramMain.CustomCounter.TOTAL_UNI_GRAM_COUNT).increment(count);
            context.getCounter(NGramMain.CustomCounter.TOTAL_BI_GRAM_COUNT).increment(count);
            context.write(key, new Text(count));
        }*/


        // logger.debug("The count of word {} is {}",key.toString(),count);


    }
}
