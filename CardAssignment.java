import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CardAssignment 
{
	public static void main(String[] args) throws Exception  
	{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf,"First Job");
	    job.setJarByClass(CardAssignment.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.setMapperClass(CardsMapper.class);
	    job.setReducerClass(CardsReducer.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	public static class CardsMapper extends Mapper<LongWritable,Text,LongWritable,Text>
	{		
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			context.write(new LongWritable(Long.parseLong(line.split(" ")[0])),new Text(line.split(" ")[1]));	
		}
	}
	public static class CardsReducer extends Reducer<LongWritable,Text,LongWritable,Text>
	{
		public void reduce(LongWritable key,Iterable<Text>values, Context context) throws IOException,InterruptedException
		{	
			String cards1 = "";
			String cards2 = "";
			String[] cards3 = {"Club","Diamond","Heart","Spade"};
			boolean b;
			for(Text value : values)
			{
				cards1 += value + " ";
			}
			for (int i=0;i<cards3.length;i++)
			{
				b = cards1.contains(cards3[i]);
				if(!b)
				{
					cards2 += cards3[i] + " ";
				}
			}
			context.write(key,new Text(cards2));	
		}
	}
	
}