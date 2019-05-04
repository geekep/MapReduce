import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * ר�������ô���ͳ��
 * @author KING
 *
 */
public class CitationCount {
	public static class PatentCitationMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		private IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// ����key: ��ƫ��ֵ��value: ��citingר����, citedר���š� ���ݶ�
			String[] citation = value.toString().split(",");
			// ���key: cited ר���ţ�value: 1
			context.write(new Text(citation[1]), one);
		} 
	}
	
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for(IntWritable val : values){
				count += val.get();
			}
			// ���key: ����ר���ţ�value: ��������
			context.write(key, new IntWritable(count));
		 } 
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job citationCountJob = new Job();
		citationCountJob.setJobName("citationCountJob");
		citationCountJob.setJarByClass(CitationCount.class);
		
		citationCountJob.setMapperClass(PatentCitationMapper.class);
		citationCountJob.setMapOutputKeyClass(Text.class);
		citationCountJob.setMapOutputValueClass(IntWritable.class);

		citationCountJob.setReducerClass(ReduceClass.class);
		citationCountJob.setOutputKeyClass(Text.class);
		citationCountJob.setOutputValueClass(IntWritable.class);

		citationCountJob.setInputFormatClass(TextInputFormat.class);
		citationCountJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(citationCountJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(citationCountJob, new Path(args[1]));
		
		citationCountJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
