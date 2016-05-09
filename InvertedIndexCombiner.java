package hw1.invertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexCombiner extends Reducer<Text,WordValue, Text,WordValue> {

	public void reduce(Text key, Iterable<WordValue> values, Context context) throws IOException, InterruptedException {
		WordValue sumValue = new WordValue();
		for (WordValue val: values) {
			sumValue.addWordValue(val);
		}
		context.write(key, sumValue);
	}
}
