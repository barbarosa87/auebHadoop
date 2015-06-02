package hadoopAuebProject;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CountTimesotReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {


	private MultipleOutputs<Text, DoubleWritable> mos;

	public void setup(Context context) {

		mos = new MultipleOutputs<Text, DoubleWritable>(context);

	}

	 
	 
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		float sum = 0;
		int counter = 0;
		double max = Double.NEGATIVE_INFINITY;
		
		//Testing functions
//		List<Double> dblList=new ArrayList<Double>();

		for (DoubleWritable value : values) {

			// Calculating sum
			sum += value.get();
			counter++;
			// Calculating max;
			if (max < value.get()) {
				max = value.get();
			}
//			dblList.add(value.get());
		}

		Float average = sum / counter;

		
		
		
		if (key.toString().contains("MONDAY") || key.toString().contains("TUESDAY")
				|| key.toString().contains("WEDNESDAY")
				|| key.toString().contains("THURSDAY") || key.toString().contains("FRIDAY")
				|| key.toString().contains("SATURDAY") || key.toString().contains("SUNDAY")) {
			mos.write(new Text(key.toString() + ";" + String.valueOf(max)),
					new DoubleWritable(average), "weekdayhistogram");
		} else if (key.toString().contains("JANUARY") || key.toString().contains("FEBRUARY")
				|| key.toString().contains("MARCH") || key.toString().contains("APRIL")
				|| key.toString().contains("MAY") || key.toString().contains("JUNE")
				|| key.toString().contains("JULY") || key.toString().contains("AUGUST")
				|| key.toString().contains("SEPTEMBER") || key.toString().contains("OCTOBER")
				|| key.toString().contains("NOVEMBER") || key.toString().contains("DECEMBER")) {
			mos.write(new Text(key.toString() + ";" + String.valueOf(max)),
					new DoubleWritable(average), "monthhistogram");
		} else {
			mos.write(new Text(key.toString() + ";" + String.valueOf(max)),
					new DoubleWritable(average), "timezonehistogram");
		}

//		 context.write(new Text(key.toString()+";"+String.valueOf(max)),new
//		 DoubleWritable(average) );

	}


	
	     public void cleanup(Context context) throws IOException, InterruptedException {
		 
		mos.close();
		 
		 }


}
