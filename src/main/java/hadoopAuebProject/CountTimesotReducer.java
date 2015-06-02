package hadoopAuebProject;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountTimesotReducer extends
		Reducer<Text, FloatWritable,Text,FloatWritable> {

	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {

		float sum = 0;
		int counter=0;
		for (FloatWritable value : values) {
			sum += value.get();
			counter++;
		}
		
		
		Float average=sum/counter;

		context.write(key,new FloatWritable(average) );

	}

}
