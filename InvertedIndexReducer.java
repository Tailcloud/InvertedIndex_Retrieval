package hw1.invertedIndex;

import java.io.IOException;

import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, WordValue, Text, Text> {
	
	public void reduce(Text key, Iterable<WordValue> values, Context context) throws IOException, InterruptedException {
		WordValue sumValue = new WordValue();
		for (WordValue val: values) {
			sumValue.addWordValue(val);
		}
		context.write(key, new Text(sumValue.toString()));
	}
}
