import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text,Text, FloatWritable > {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	public static final String startTime1 = "00:00";
	public static final String endTimeslot1 = "06:00";
	public static final String startTime2 = "06:00";
	public static final String endTimeslot2 = "12:00";
	public static final String startTime3 = "12:00";
	public static final String endTimeslot3 = "18:00";
	public static final String startTime4 = "18:00";
	public static final String endTimeslot4 = "00:00";

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line);

		while (itr.hasMoreTokens()) {

			word.set(itr.nextToken());
			String strWord = word.toString();
			String[] splitLine = strWord.split(";");

			if ((splitLine[0].contains("2007") || splitLine[0].contains("2008") || splitLine[0]
					.contains("2009"))
					&& !word.toString().contains("?")) {

				String[] splitPerHours = splitLine[1].split(":");

				if (Integer.parseInt(splitPerHours[0]) > 0
						&& Integer.parseInt(splitPerHours[0]) < 6) {
//					context.write(new FloatWritable(
//							Float.parseFloat((splitLine[6]))), new Text("00:00-06:00"));
					context.write(new Text("00:00-06:00SubMet1"), new FloatWritable(
							Float.parseFloat((splitLine[6]))));
					context.write(new Text("00:00-06:00SubMet2"), new FloatWritable(
							Float.parseFloat((splitLine[7]))));
					context.write(new Text("00:00-06:00SubMet3"), new FloatWritable(
							Float.parseFloat((splitLine[8]))));
				}
			}

		}

	}

}
