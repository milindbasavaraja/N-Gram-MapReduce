package com.nyu.bigdata.Mapper;

import com.nyu.bigdata.NGramMain;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BiGramLMProbabilityMapper extends Mapper<LongWritable, Text,Text, DoubleWritable> {

    private final Logger logger = LoggerFactory.getLogger(BiGramLMProbabilityMapper.class);

    @Override
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        //logger.info("The key is: {} and value is:{} ",key,value);
        String biGramDetails = value.toString().split("\t")[1];
        String uniGramDetails = value.toString().split("\t")[2];
        long uniGramWordCount = Long.parseLong(uniGramDetails.split(" ")[1]);



        if(biGramDetails.split(" ").length > 2){
            String[] bigGramWordDetails = biGramDetails.split(" ");
            String biGramWord = bigGramWordDetails[0] + " "+bigGramWordDetails[1];
            long biGramWordCount = Long.parseLong(bigGramWordDetails[2]);

            long totalBiGramWord = context.getConfiguration().getLong(NGramMain.CustomCounter.TOTAL_BI_GRAM_COUNT.name(), 0);
            long totalUniGramWord = context.getConfiguration().getLong(NGramMain.CustomCounter.TOTAL_UNI_GRAM_COUNT.name(), 0);

            double probabilityW1W2 = (double)biGramWordCount/totalBiGramWord;
            double probabilityW1 = (double)uniGramWordCount/totalUniGramWord;
            double finalProbability = probabilityW1W2/probabilityW1;
            context.write(new Text(biGramWord),new DoubleWritable(finalProbability));


        }

    }
}
