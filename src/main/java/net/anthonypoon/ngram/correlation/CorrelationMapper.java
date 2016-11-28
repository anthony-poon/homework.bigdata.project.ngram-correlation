/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.anthonypoon.ngram.correlation;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ypoon
 */
public class CorrelationMapper extends Mapper<Text, Text, Text, Text>{
    Integer lowbound = 0;
    Integer upbound = 9999;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        lowbound = Integer.valueOf(context.getConfiguration().get("lowbound"));
        upbound = Integer.valueOf(context.getConfiguration().get("upbound"));
    }

    
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArray = value.toString().split("\t");
        if (lowbound <= Integer.valueOf(strArray[0]) && upbound >= Integer.valueOf(strArray[0])) {
            context.write(key, new Text(strArray[0] + "\t" + strArray[1]));
        }
    }
    
}
