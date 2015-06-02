import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, FloatWritable> {

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

			try {

				word.set(itr.nextToken());
				String strWord = word.toString();
				String[] splitLine = strWord.split(";");

				if ((splitLine[0].contains("2007")
						|| splitLine[0].contains("2008") || splitLine[0]
							.contains("2009"))
						&& !word.toString().contains("?")) {

					String[] splitPerHours = splitLine[1].split(":");
					String[] splitPerMonth = splitLine[0].split("/");

					// Get Week Day

					Date date = new SimpleDateFormat("dd/mm/yyyy")
							.parse(splitLine[0]);
					Calendar cal = Calendar.getInstance();
					cal.setTime(date);
					int dayofWeek = cal.get(Calendar.DAY_OF_WEEK);

					if (Integer.parseInt(splitPerHours[0]) > 0
							&& Integer.parseInt(splitPerHours[0]) < 6) {
						// context.write(new FloatWritable(
						// Float.parseFloat((splitLine[6]))), new
						// Text("00:00-06:00"));

						// Create key strings
						String writeTextSubMet1 = String.valueOf(dayofWeek)
								+";"+ splitPerMonth[1] + ";00:00-06:00SubMet1";
						String writeTextSubMet2 = String.valueOf(dayofWeek)
								+";"+ splitPerMonth[1] + ";00:00-06:00SubMet2";
						String writeTextSubMet3 = String.valueOf(dayofWeek)
								+";"+ splitPerMonth[1] + ";00:00-06:00SubMet3";

						// String writeTextSubMet1 = splitPerMonth[1]
						// + ";00:00-06:00SubMet1";
						// String writeTextSubMet2 = splitPerMonth[1]
						// + ";00:00-06:00SubMet2";
						// String writeTextSubMet3 = splitPerMonth[1]
						// + ";00:00-06:00SubMet3";

						context.write(
								new Text(writeTextSubMet1),
								new FloatWritable(Float
										.parseFloat((splitLine[6]))));
						context.write(
								new Text(writeTextSubMet2),
								new FloatWritable(Float
										.parseFloat((splitLine[7]))));
						context.write(
								new Text(writeTextSubMet3),
								new FloatWritable(Float
										.parseFloat((splitLine[8]))));
					
					
					} else if (Integer.parseInt(splitPerHours[0]) > 6
							&& Integer.parseInt(splitPerHours[0]) < 12) {
						// context.write(new FloatWritable(
						// Float.parseFloat((splitLine[6]))), new
						// Text("00:00-06:00"));

						// Create key strings
						String writeTextSubMet1 = String.valueOf(dayofWeek)
								+";"+ splitPerMonth[1] + ";06:00-12:00SubMet1";
						String writeTextSubMet2 = String.valueOf(dayofWeek)
								+";"+ splitPerMonth[1] + ";06:00-12:00SubMet2";
						String writeTextSubMet3 = String.valueOf(dayofWeek)
								+";"+ splitPerMonth[1] + ";06:00-12:00SubMet3";
						context.write(
								new Text(writeTextSubMet1),
								new FloatWritable(Float
										.parseFloat((splitLine[6]))));
						context.write(
								new Text(writeTextSubMet2),
								new FloatWritable(Float
										.parseFloat((splitLine[7]))));
						context.write(
								new Text(writeTextSubMet3),
								new FloatWritable(Float
										.parseFloat((splitLine[8]))));
					} else if (Integer.parseInt(splitPerHours[0]) > 12
							&& Integer.parseInt(splitPerHours[0]) < 18) {
						// context.write(new FloatWritable(
						// Float.parseFloat((splitLine[6]))), new
						// Text("00:00-06:00"));

						// Create key strings
						String writeTextSubMet1 = String.valueOf(dayofWeek)
								+";"+ splitPerMonth[1] + ";12:00-18:00SubMet1";
						String writeTextSubMet2 = String.valueOf(dayofWeek)
								+";"+ splitPerMonth[1] + ";12:00-18:00SubMet2";
						String writeTextSubMet3 = String.valueOf(dayofWeek)
								+";"+ splitPerMonth[1] + ";12:00-18:00SubMet3";

						context.write(
								new Text(writeTextSubMet1),
								new FloatWritable(Float
										.parseFloat((splitLine[6]))));
						context.write(
								new Text(writeTextSubMet2),
								new FloatWritable(Float
										.parseFloat((splitLine[7]))));
						context.write(
								new Text(writeTextSubMet3),
								new FloatWritable(Float
										.parseFloat((splitLine[8]))));

					} else if (Integer.parseInt(splitPerHours[0]) > 18
							&& Integer.parseInt(splitPerHours[0]) < 0) {
						// context.write(new FloatWritable(
						// Float.parseFloat((splitLine[6]))), new
						// Text("00:00-06:00"));

						// Create key strings
						String writeTextSubMet1 = String.valueOf(dayofWeek)
								+";"+ splitPerMonth[1] + ";18:00-00:00SubMet1";
						String writeTextSubMet2 = String.valueOf(dayofWeek)
								+";"+ splitPerMonth[1] + ";18:00-00:00SubMet2";
						String writeTextSubMet3 = String.valueOf(dayofWeek)
								+";"+ splitPerMonth[1] + ";18:00-00:00SubMet3";
						
						context.write(
								new Text(writeTextSubMet1),
								new FloatWritable(Float
										.parseFloat((splitLine[6]))));
						context.write(
								new Text(writeTextSubMet2),
								new FloatWritable(Float
										.parseFloat((splitLine[7]))));
						context.write(
								new Text(writeTextSubMet3),
								new FloatWritable(Float
										.parseFloat((splitLine[8]))));
					}
				}

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
