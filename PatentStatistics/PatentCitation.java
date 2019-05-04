import java.io.IOException;

import org.apache.hadoop.fs.Path;
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
 * ����ר�������б�����ר�����ù�ϵ�Ĺ�ϵ��(patentNo1,patentNo2)
 * �ļ������ÿ��ר���ŵ������õ��ļ����Զ��������
 * @author KING
 *
 */
public class PatentCitation {
	public static class PatentCitationMapper extends Mapper<LongWritable,Text,Text,Text>{
		/**
		 * �����λ��ƫ�ƣ�ֵΪ��ר����1,ר����2��
		 */
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] citation = value.toString().split(",");
			context.write(new Text(citation[1]), new Text(citation[0]));
		} 
	}
	
	public static class PatentCitationReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder csv = new StringBuilder("");
			for (Text val:values) {
				if (csv.length() > 0) {
				csv.append(",");
				}
				csv.append(val.toString());
			}
			context.write(key, new Text(csv.toString()));
		 } 
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job patentCitationJob = new Job();
		patentCitationJob.setJobName("patentCitationJob");
		patentCitationJob.setJarByClass(PatentCitation.class);
		
		patentCitationJob.setMapperClass(PatentCitationMapper.class);
		patentCitationJob.setMapOutputKeyClass(Text.class);
		patentCitationJob.setMapOutputValueClass(Text.class);

		patentCitationJob.setReducerClass(PatentCitationReducer.class);
		patentCitationJob.setOutputKeyClass(Text.class);
		patentCitationJob.setOutputValueClass(Text.class);

		patentCitationJob.setInputFormatClass(TextInputFormat.class);
		patentCitationJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(patentCitationJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(patentCitationJob, new Path(args[1]));
		
		patentCitationJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
