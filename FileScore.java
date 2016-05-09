package hw1.retrieval;

import java.io.*;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

public class FileScore implements WritableComparable {
	//private static String inputPath = "./input";
	//private static String inputPath = "hdfs://pp11:9000/demo/HW1/demo2";
	private Text word;
	private StringBuffer offsets;
	private double tw;

	public FileScore() {
		word = new Text();
		offsets = new StringBuffer();
	}
	
	public FileScore(String word, String offsets, double tw) {
		this.word = new Text(word);
		this.offsets = new StringBuffer(offsets);
		this.tw = tw;
	}
	
	public void add(FileScore fs) {
		tw += fs.getTw();
		String fsoffs = fs.getOffsets();
		//String offs = fsoffs.substring(1, fsoffs.length()-1);
		String oroffs = offsets.toString();
		//String suboff = oroffs.substring(1, oroffs.length()-1);
		offsets.append(oroffs+","+fsoffs);
	}
	
	public String getWord(){
		return word.toString();
	}
	
	public String getOffsets(){
		return offsets.toString();
	}
	
	public double getTw() {
		return tw;
	}
	
	public String getFileFrag(int fileId, String filePath) throws IOException{
		Path path = getFilePath(fileId, filePath);
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataInputStream fileReader = fs.open(path);
		// get all offset array of each word
		StringBuffer frag= new StringBuffer();
		StringTokenizer tokens = new StringTokenizer(offsets.toString(), "[,]");
		while(tokens.hasMoreTokens()) {
			int offset = Integer.parseInt(tokens.nextToken());
			byte[] buf = new byte[50];
			fileReader.read(offset-20, buf, 0, 50);
			String bufStr = (new String(buf)).replace("\n", " ");
			frag.append("\t\t..."+bufStr+"...\n");
		}
		return frag.toString();
	}
	
	public Path getFilePath(int fid, String filePath) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path(filePath);
		if (!fileSystem.exists(path)) {
			return null;
		}
		FileStatus[] status = fileSystem.listStatus(path);
		Arrays.sort(status);
		return status[fid].getPath();
    }
	
	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
		new Text(offsets.toString()).write(out);
		out.writeDouble(tw);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		Text offsetTex = new Text();
		offsetTex.readFields(in);
		offsets = new StringBuffer(offsetTex.toString());
		tw = in.readDouble();
	}
	
	@Override
	public int compareTo(Object o) {
		FileScore ofs = (FileScore)o;
		double diff = tw-ofs.getTw();
		if(diff>0)
			return 1;
		else if (diff<0)
			return -1;
		else
			return 0;
	}
}
