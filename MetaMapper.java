package hw1.invertedIndex;

import java.io.IOException;

import java.io.*;
import java.util.regex.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;


public class MetaMapper extends Mapper<LongWritable, Text, Text, Term> {
	//String inputPath = "hdfs://pp11:9000/demo/HW1/demo2";
	int total = 0;
	String filename = "";
	String word;
	HashMap<String, Integer> fileIdMap = new HashMap<String, Integer>();
	
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = ((FileSplit) context.getInputSplit()).getPath().getParent();
		FileStatus[] fstatus = fileSystem.listStatus(path);
		Arrays.sort(fstatus);
		for(int i=0; i<fstatus.length; i++){
			path = fstatus[i].getPath();
			fileIdMap.put(path.getName(), new Integer(i));
		}
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Pattern ptn = Pattern.compile("[A-Za-z]+");
		Matcher mat = ptn.matcher(value.toString());
		filename = ((FileSplit) context.getInputSplit()).getPath().getName();
    
		while (mat.find()) {
        word = fileIdMap.get(filename).intValue() + " " + mat.group();
			total++;
			int offset = (int) key.get() + mat.start();
			context.write(new Text(word), new Term(offset));
		}
	}
}
