/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.anthonypoon.ngram.correlation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author ypoon
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("a", "action", true, "Action");
        options.addOption("i", "input", true, "input");
        options.addOption("o", "output", true, "output");
        //options.addOption("f", "format", true, "Format");
        options.addOption("u", "upbound", true, "Year up bound");
        options.addOption("l", "lowbound", true, "Year low bound");
        options.addOption("t", "target", true, "Correlation Target URI");   // Can only take file from S# or HDFS
        options.addOption("L", "lag", true, "Lag factor");
        options.addOption("T", "threshold", true, "Correlation threshold");
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = parser.parse(options, args);
        Configuration conf = new Configuration();
        if (cmd.hasOption("lag")) {
            conf.set("lag", cmd.getOptionValue("lag"));
        }
        if (cmd.hasOption("threshold")) {
            conf.set("threshold", cmd.getOptionValue("threshold"));
        }
        if (cmd.hasOption("upbound")) {
            conf.set("upbound", cmd.getOptionValue("upbound"));
        } else {
            conf.set("upbound", "9999");
        }
        if (cmd.hasOption("lowbound")) {
            conf.set("lowbound", cmd.getOptionValue("lowbound"));
        } else {
            conf.set("lowbound", "0");
        }
        if (cmd.hasOption("target")) {
            conf.set("target", cmd.getOptionValue("target"));
        } else {
            throw new Exception("Missing correlation target file uri");
        }
        Job job = Job.getInstance(conf);
        /**
        if (cmd.hasOption("format")) {
            switch (cmd.getOptionValue("format")) {
                case "compressed":
                    job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
                    break;
                case "text":
                    job.setInputFormatClass(KeyValueTextInputFormat.class);
                    break;
            }
            
        }**/
        job.setJarByClass(Main.class);
        switch (cmd.getOptionValue("action")) {
            case "get-correlation":
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                for (String inputPath : cmd.getOptionValue("input").split(",")) {
                    MultipleInputs.addInputPath(job,new Path(inputPath), KeyValueTextInputFormat.class,CorrelationMapper.class);
                }
                job.setReducerClass(CorrelationReducer.class);
                break;            
            default:
                throw new IllegalArgumentException("Missing action");
        }
        
        String timestamp = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new Date());       
        
        //FileInputFormat.setInputPaths(job, new Path(cmd.getOptionValue("input")));
        FileOutputFormat.setOutputPath(job, new Path(cmd.getOptionValue("output") + "/" + timestamp));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
