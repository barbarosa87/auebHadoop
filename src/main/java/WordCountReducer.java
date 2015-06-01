import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
		Reducer<Text, FloatWritable,Text,FloatWritable> {

	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {

		float sum = 0;
		for (FloatWritable value : values) {
			sum += value.get();
		}
		

		context.write(key,new FloatWritable(sum) );

	}

}
