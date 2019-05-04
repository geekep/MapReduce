import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * ר�������ô����ֲ�ͳ�ƣ������ļ�Ϊר�����ô���ͳ�Ƶ�������
 * ɨ���ļ�����ר���ţ��������Ǳ����õĴ�����ͳ��ÿһ�������ֱ�
 * �ж��ٴγ��֡�
 * 
 * @author KING
 *
 */
public class CitationCountDistribution {
	public static class MapClass extends Mapper<Text, Text, IntWritable, IntWritable> {
		private IntWritable one = new IntWritable(1);
		
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			IntWritable citationCount = new IntWritable(Integer.parseInt(value.toString()));
			context.write (citationCount, one);
	     }
	}
	
	public static class ReduceClass extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for(IntWritable val : values){
				count += val.get();
			}
			 // ���key: ����������value: �ܳ��ִ���
			context.write(key, new IntWritable(count));
			}
		}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job citationCountDistributionJob = new Job();
		citationCountDistributionJob.setJobName("citationCountDistributionJob");
		citationCountDistributionJob.setJarByClass(CitationCountDistribution.class);
		
		citationCountDistributionJob.setMapperClass(MapClass.class);
		citationCountDistributionJob.setMapOutputKeyClass(IntWritable.class);
		citationCountDistributionJob.setMapOutputValueClass(IntWritable.class);

		citationCountDistributionJob.setReducerClass(ReduceClass.class);
		citationCountDistributionJob.setOutputKeyClass(IntWritable.class);
		citationCountDistributionJob.setOutputValueClass(IntWritable.class);

		citationCountDistributionJob.setInputFormatClass(TextInputFormat.class);
		citationCountDistributionJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(citationCountDistributionJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(citationCountDistributionJob, new Path(args[1]));
		
		citationCountDistributionJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
