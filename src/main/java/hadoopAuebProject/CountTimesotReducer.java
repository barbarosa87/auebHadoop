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
		float max=500;
		for (FloatWritable value : values) {
			
			//Calculating sum
			sum += value.get();
			counter++;
			//Calculating max;
			if(max>value.get()){
				max=value.get();
			}
		}
		
		
		Float average=sum/counter;

		context.write(new Text(key.toString()+";"+String.valueOf(max)),new FloatWritable(average) );

	}

}
