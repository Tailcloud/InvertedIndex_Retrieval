package hw1.invertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MetaCombiner extends Reducer<Text,Term,Text,Term> {

	public void reduce(Text key, Iterable<Term> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		StringBuffer output = new StringBuffer("");
    
		for (Term val: values) {
			if(output.length()!=0)
				output.append(",");
			output.append(val.getOffsets());
			count += val.getCount();
		}
		output.append("");
		
		Term term = new Term();
		term.addOffset(output.toString(), count);
		context.write(key, term);
	}
}
