package hw1.invertedIndex;

import java.io.IOException;

import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MetaReducer extends Reducer<Text, Term, Text, Text> {
	
	public void reduce(Text key, Iterable<Term> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		StringBuffer output = new StringBuffer("");
    
		String[] keySet = key.toString().split(" ");
		int fileId = Integer.parseInt(keySet[0]);
    
		for (Term val: values) {
			if(output.length()!=1)
				output.append(",");
			output.append(val.getOffsets());
			count += val.getCount();
		}
		
		Term term = new Term();
		term.addOffset(output.toString(), count);
		context.write(new Text(fileId+" "+keySet[1]), new Text(term.toString()));
	}
}
