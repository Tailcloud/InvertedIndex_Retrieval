package hw1.retrieval;

import java.io.IOException;

import java.text.*;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class RetrievalReducer extends Reducer<Text, FileScore, Text, Text> {
	CmdParser cmdParser;
	TreeMap<FileScore, String> fileScoreTbl;
	NumberFormat formatter = new DecimalFormat("0.0000000000000");
	String filePath;
	
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		cmdParser = new CmdParser(conf.get("cmd"));
		fileScoreTbl = new TreeMap<FileScore, String>();
		filePath = conf.get("filePath");
	}
	
	public void reduce(Text key, Iterable<FileScore> values, Context context) throws IOException, InterruptedException {
		String fileId = key.toString();
		String output;
		FileScore sum = new FileScore();
		for(FileScore fs : values) {
			sum.add(fs);
		}
		if(fileScoreTbl.size()<10 || isBigger(sum))
			fileScoreTbl.put(sum, fileId);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		StringBuffer output = new StringBuffer("\""+cmdParser.getCmd()+"\" : \n");
		int i=1;
		for(FileScore fs : fileScoreTbl.descendingKeySet()){
			String fid = fileScoreTbl.get(fs);
			output.append("No."+i+" : file"+fid+"\t"+formatter.format(fs.getTw())+"\n"+fs.getFileFrag(Integer.parseInt(fid), filePath));
			i++;
		}
		context.write(new Text("search"), new Text(output.toString()));
	}
	
	private boolean isBigger(FileScore fs) {
		FileScore min = fileScoreTbl.firstKey();
		return fs.getTw() > min.getTw();
	}
}