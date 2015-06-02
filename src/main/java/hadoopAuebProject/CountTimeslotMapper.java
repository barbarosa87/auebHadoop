package hadoopAuebProject;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountTimeslotMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {

	private Text word = new Text();

	// public static final String startTime1 = "00:00";
	// public static final String endTimeslot1 = "06:00";
	// public static final String startTime2 = "06:00";
	// public static final String endTimeslot2 = "12:00";
	// public static final String startTime3 = "12:00";
	// public static final String endTimeslot3 = "18:00";
	// public static final String startTime4 = "18:00";
	// public static final String endTimeslot4 = "00:00";

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
					//String[] splitPerMonth = splitLine[0].split("/");

					// Get Week Day
					Date date = new SimpleDateFormat("dd/MM/yyyy",Locale.ENGLISH).parse(splitLine[0]);
					
					Calendar cal = Calendar.getInstance();
					cal.setTime(date);
					int dayofWeek = cal.get(Calendar.DAY_OF_WEEK);
					int monthNumeric = cal.get(Calendar.MONTH);

					// String representation of DAY
					String day;
					// Translate day and month to STRING
					switch (dayofWeek) {
					case 1:
						day = "SUNDAY";
						break;
					case 2:
						day = "MONDAY";
						break;
					case 3:
						day = "TUESDAY";
						break;
					case 4:
						day = "WEDNESDAY";
						break;
					case 5:
						day = "THURSDAY";
						break;
					case 6:
						day = "FRIDAY";
						break;
					case 7:
						day = "SATURDAY";
						break;
					default:
						day = "UNDEFINED";
						break;
					}

					// String representation of MONTH
					String month;

					switch (monthNumeric) {
					case 0:
						month = "JANUARY";
						break;
					case 1:
						month = "FEBRUARY";
						break;
					case 2:
						month = "MARCH";
						break;
					case 3:
						month = "APRIL";
						break;
					case 4:
						month = "MAY";
						break;
					case 5:
						month = "JUNE";
						break;
					case 6:
						month = "JULY";
						break;
					case 7:
						month = "AUGUST";
						break;
					case 8:
						month = "SEPTEMBER";
						break;
					case 9:
						month = "OCTOBER";
						break;
					case 10:
						month = "NOVEMBER";
						break;
					case 11:
						month = "DECEMBER";
						break;
					default:
						month = "UNDEFINED";
						break;
					}

					if (Integer.parseInt(splitPerHours[0]) > 0
							&& Integer.parseInt(splitPerHours[0]) < 6) {
						
						 String writeTextSubMet1 = "00:00-06:00SubMet1";
						 String writeTextSubMet2 = "00:00-06:00SubMet2";
						 String writeTextSubMet3 = "00:00-06:00SubMet3";
						
						 context.write(
						 new Text(writeTextSubMet1),
						 new DoubleWritable(Float
						 .parseFloat((splitLine[6]))));
						 context.write(
						 new Text(writeTextSubMet2),
						 new DoubleWritable(Float
						 .parseFloat((splitLine[7]))));
						 context.write(
						 new Text(writeTextSubMet3),
						 new DoubleWritable(Float
						 .parseFloat((splitLine[8]))));
						 
						 
					} else if (Integer.parseInt(splitPerHours[0]) > 6
							&& Integer.parseInt(splitPerHours[0]) < 12) {
						
						 String writeTextSubMet1 = "06:00-12:00SubMet1";
						 String writeTextSubMet2 = "06:00-12:00SubMet2";
						 String writeTextSubMet3 = "06:00-12:00SubMet3";
						
						 context.write(
						 new Text(writeTextSubMet1),
						 new DoubleWritable(Float
						 .parseFloat((splitLine[6]))));
						 context.write(
						 new Text(writeTextSubMet2),
						 new DoubleWritable(Float
						 .parseFloat((splitLine[7]))));
						 context.write(
						 new Text(writeTextSubMet3),
						 new DoubleWritable(Float
						 .parseFloat((splitLine[8]))));
						 
					} else if (Integer.parseInt(splitPerHours[0]) > 12
							&& Integer.parseInt(splitPerHours[0]) < 18) {
						
						 String writeTextSubMet1 = "12:00-18:00SubMet1";
						 String writeTextSubMet2 = "12:00-18:00SubMet2";
						 String writeTextSubMet3 = "12:00-18:00SubMet3";
						
						 context.write(
						 new Text(writeTextSubMet1),
						 new DoubleWritable(Float
						 .parseFloat((splitLine[6]))));
						 context.write(
						 new Text(writeTextSubMet2),
						 new DoubleWritable(Float
						 .parseFloat((splitLine[7]))));
						 context.write(
						 new Text(writeTextSubMet3),
						 new DoubleWritable(Float
						 .parseFloat((splitLine[8]))));

					} else if (Integer.parseInt(splitPerHours[0]) > 18
							&& Integer.parseInt(splitPerHours[0]) < 0) {
						
						 String writeTextSubMet1 = "18:00-00:00SubMet1";
						 String writeTextSubMet2 = "18:00-00:00SubMet2";
						 String writeTextSubMet3 = "18:00-00:00SubMet3";
						
						 context.write(
						 new Text(writeTextSubMet1),
						 new DoubleWritable(Float
						 .parseFloat((splitLine[6]))));
						 context.write(
						 new Text(writeTextSubMet2),
						 new DoubleWritable(Float
						 .parseFloat((splitLine[7]))));
						 context.write(
						 new Text(writeTextSubMet3),
						 new DoubleWritable(Float
						 .parseFloat((splitLine[8]))));

					}
                    int j=6;
					for(int i=0;i<3;i++){
						context.write(
								 new Text(day+"Submet"+String.valueOf(i+1)),
								 new DoubleWritable(Float
								 .parseFloat((splitLine[j]))));
						j++;
					}
					
					j=6;
					for(int i=0;i<3;i++){
						context.write(
								 new Text(month+"Submet"+String.valueOf(i+1)),
								 new DoubleWritable(Float
								 .parseFloat((splitLine[j]))));
						j++;
					}
					
					// if (Integer.parseInt(splitPerHours[0]) > 0
					// && Integer.parseInt(splitPerHours[0]) < 6) {
					// // context.write(new DoubleWritable(
					// // Float.parseFloat((splitLine[6]))), new
					// // Text("00:00-06:00"));
					//
					// // Create key strings
					// String writeTextSubMet1 = "00:00-06:00SubMet1";
					// String writeTextSubMet2 = "00:00-06:00SubMet2";
					// String writeTextSubMet3 = "00:00-06:00SubMet3";
					//
					// // String writeTextSubMet1 = splitPerMonth[1]
					// // + ";00:00-06:00SubMet1";
					// // String writeTextSubMet2 = splitPerMonth[1]
					// // + ";00:00-06:00SubMet2";
					// // String writeTextSubMet3 = splitPerMonth[1]
					// // + ";00:00-06:00SubMet3";
					//
					// context.write(
					// new Text(writeTextSubMet1),
					// new DoubleWritable(Float
					// .parseFloat((splitLine[6]))));
					// context.write(
					// new Text(writeTextSubMet2),
					// new DoubleWritable(Float
					// .parseFloat((splitLine[7]))));
					// context.write(
					// new Text(writeTextSubMet3),
					// new DoubleWritable(Float
					// .parseFloat((splitLine[8]))));
					//
					//
					// } else if (Integer.parseInt(splitPerHours[0]) > 6
					// && Integer.parseInt(splitPerHours[0]) < 12) {
					// // context.write(new DoubleWritable(
					// // Float.parseFloat((splitLine[6]))), new
					// // Text("00:00-06:00"));
					//
					// // Create key strings
					// String writeTextSubMet1 = "06:00-12:00SubMet1";
					// String writeTextSubMet2 = "06:00-12:00SubMet2";
					// String writeTextSubMet3 = "06:00-12:00SubMet3";
					// context.write(
					// new Text(writeTextSubMet1),
					// new DoubleWritable(Float
					// .parseFloat((splitLine[6]))));
					// context.write(
					// new Text(writeTextSubMet2),
					// new DoubleWritable(Float
					// .parseFloat((splitLine[7]))));
					// context.write(
					// new Text(writeTextSubMet3),
					// new DoubleWritable(Float
					// .parseFloat((splitLine[8]))));
					// } else if (Integer.parseInt(splitPerHours[0]) > 12
					// && Integer.parseInt(splitPerHours[0]) < 18) {
					// // context.write(new DoubleWritable(
					// // Float.parseFloat((splitLine[6]))), new
					// // Text("00:00-06:00"));
					//
					// // Create key strings
					// String writeTextSubMet1 = "12:00-18:00SubMet1";
					// String writeTextSubMet2 = "12:00-18:00SubMet2";
					// String writeTextSubMet3 = "12:00-18:00SubMet3";
					//
					// context.write(
					// new Text(writeTextSubMet1),
					// new DoubleWritable(Float
					// .parseFloat((splitLine[6]))));
					// context.write(
					// new Text(writeTextSubMet2),
					// new DoubleWritable(Float
					// .parseFloat((splitLine[7]))));
					// context.write(
					// new Text(writeTextSubMet3),
					// new DoubleWritable(Float
					// .parseFloat((splitLine[8]))));
					//
					// } else if (Integer.parseInt(splitPerHours[0]) > 18
					// && Integer.parseInt(splitPerHours[0]) < 0) {
					// // context.write(new DoubleWritable(
					// // Float.parseFloat((splitLine[6]))), new
					// // Text("00:00-06:00"));
					//
					// // Create key strings
					// String writeTextSubMet1 = "18:00-00:00SubMet1";
					// String writeTextSubMet2 = "18:00-00:00SubMet2";
					// String writeTextSubMet3 = "18:00-00:00SubMet3";
					//
					// context.write(
					// new Text(writeTextSubMet1),
					// new DoubleWritable(Float
					// .parseFloat((splitLine[6]))));
					// context.write(
					// new Text(writeTextSubMet2),
					// new DoubleWritable(Float
					// .parseFloat((splitLine[7]))));
					// context.write(
					// new Text(writeTextSubMet3),
					// new DoubleWritable(Float
					// .parseFloat((splitLine[8]))));
					// }
				}

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
