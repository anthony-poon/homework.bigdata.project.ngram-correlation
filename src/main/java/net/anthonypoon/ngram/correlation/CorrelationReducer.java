/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.anthonypoon.ngram.correlation;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ypoon
 */
public class CorrelationReducer extends Reducer<Text, Text, Text, Text>{
    private double threshold = 0.7;
    private Integer lowbound = 0;
    private Integer upbound = 0;
    private Integer lag = 0;
    private Map<String, TreeMap<String, Double>> corrTargetArray = new HashMap();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        threshold = Double.valueOf(context.getConfiguration().get("threshold", "0.7"));
        lag = Integer.valueOf(context.getConfiguration().get("lag", "0"));
        lowbound = Integer.valueOf(context.getConfiguration().get("lowbound"));
        upbound = Integer.valueOf(context.getConfiguration().get("upbound"));
        String uri = context.getConfiguration().get("target");
        Path input = new Path(uri);
        // Read data from path to be our compare target
        FileSystem fileSystem = input.getFileSystem(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(input)));
        String line = "";
        while ((line = br.readLine()) != null) {            
            String[] strArray = line.split("\t");
            if (Integer.valueOf(strArray[1]) <= upbound && Integer.valueOf(strArray[1]) >= lowbound) {
                if (!corrTargetArray.containsKey(strArray[0])) {
                    corrTargetArray.put(strArray[0], new TreeMap());
                }
                TreeMap<String, Double> targetElement = corrTargetArray.get(strArray[0]);
                targetElement.put(strArray[1], Double.valueOf(strArray[2]));
            }
        }
        
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeMap<String, Double> currElement = new TreeMap();
        for (Text val : values) {
            String[] strArray = val.toString().split("\t");
            currElement.put(strArray[0], Double.valueOf(strArray[1]));
        }
        double[] currElementPrimitve = new double[upbound - lowbound + 1];
        for (Integer i = 0; i <= upbound - lowbound; i ++) {
            if (currElement.containsKey(String.valueOf(lowbound + i - lag))) {
                currElementPrimitve[i] = currElement.get(String.valueOf(lowbound + i - lag));
            } else {
                currElementPrimitve[i] = 0;
            }
            
        }
        for (Map.Entry<String, TreeMap<String, Double>> pair : corrTargetArray.entrySet()) {
            double[] targetElemetPrimitive = new double[upbound - lowbound + 1];
            for (Integer i = 0; i <= upbound - lowbound; i ++) {
                if (pair.getValue().containsKey(String.valueOf(lowbound + i))) {
                    targetElemetPrimitive[i] = pair.getValue().get(String.valueOf(lowbound + i));
                } else {
                    targetElemetPrimitive[i] = 0;
                }
            }
            Double correlation = new PearsonsCorrelation().correlation(targetElemetPrimitive, currElementPrimitve);
            if (correlation > threshold) {
                NumberFormat formatter = new DecimalFormat("#0.000");
                context.write(key, new Text(pair.getKey() + "\t" + formatter.format(correlation)));
            }
        }
        
    }

    
    
}
