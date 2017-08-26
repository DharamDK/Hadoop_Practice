package reducerPack;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceGrants extends Reducer<Text, IntWritable, Text, FloatWritable>{

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int count=0;
		int total=0;
		for(IntWritable i : values){
			count = count + i.get();
			total++;
		}
		context.write(key, new FloatWritable((float) count/total));
	}
}
