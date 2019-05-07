import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class NaiveBayesMain
{
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);
		Path path_train, path_temp, path_test, path_out;
		if(otherArgs.length != 5)
		{
			System.err.println("Usage: NaiveBayesMain <dfs_path> <conf> <train> <test> <out>");
			System.exit(2);
		}
		
		conf.set("conf", otherArgs[0] + "/" +otherArgs[1]);
		conf.set("train", otherArgs[0] + "/" +otherArgs[2]);
		conf.set("test", otherArgs[0] + "/" +otherArgs[3]);
		conf.set("output", otherArgs[0] + "/" +otherArgs[4]);
		
		// 分别将当前目录下的NBayes.conf, NBayes.train, NBayes.test上传到 HDFS的指定目录
    	put2HDFS(otherArgs[1], otherArgs[0] + "/" + otherArgs[1], conf);
    	put2HDFS(otherArgs[2], otherArgs[0] + "/" + otherArgs[2], conf);
    	put2HDFS(otherArgs[3], otherArgs[0] + "/" + otherArgs[3], conf);
		
    	// 根据主函数传入的参数分别设置 4个路径
		path_train = new Path(otherArgs[0] + "/" + otherArgs[2]);
    	path_temp = new Path(otherArgs[0] + "/" + otherArgs[2] + ".train");
    	path_test = new Path(otherArgs[0] + "/" +otherArgs[3]);
    	path_out = new Path(otherArgs[0] + "/" + otherArgs[4]);
    	
    	// 朴素贝叶斯的训练过程
		{
		Job job_train = Job.getInstance(conf, "naive bayse training");
		job_train.setJarByClass(NaiveBayesMain.class);
		job_train.setMapperClass(NaiveBayesTrain.TrainMapper.class);
		job_train.setCombinerClass(NaiveBayesTrain.TrainReducer.class);
		job_train.setReducerClass(NaiveBayesTrain.TrainReducer.class);
		job_train.setOutputKeyClass(Text.class);
    	job_train.setOutputValueClass(IntWritable.class);
     	
    	FileInputFormat.setInputPaths(job_train, path_train);
    	// 保证 HDFS上的临时文件事先不存在
    	if(fs.exists(path_temp))
    		fs.delete(path_temp, true);
    	FileOutputFormat.setOutputPath(job_train, path_temp);
    	if(job_train.waitForCompletion(true) == false)
    		System.exit(1);
    		
    	conf.set("train_result", otherArgs[0] + "/" +otherArgs[2] + ".train");
    	}
		
		// 朴素贝叶斯的测试过程
    	{
    	Job job_test = Job.getInstance(conf, "naive bayse testing");
    	job_test.setJarByClass(NaiveBayesTest.class);
    	job_test.setMapperClass(NaiveBayesTest.TestMapper.class);
    	job_test.setOutputKeyClass(Text.class);
    	job_test.setOutputValueClass(Text.class);
    	
    	FileInputFormat.setInputPaths(job_test, path_test);
    	// 保证 HDFS上的输出路径事先不存在
    	if(fs.exists(path_out))
    		fs.delete(path_out, true);
    	FileOutputFormat.setOutputPath(job_test, path_out);
    	
    	if(job_test.waitForCompletion(true) == false)
    		System.exit(1);
    	// 删除临时文件
    	}
    	// 将 HDFS上的临时文件和输出文件夹保存到本地文件系统，并且删除 HDFS上的临时文件和输出文件夹
    	getFromHDFS(otherArgs[0] + "/" + otherArgs[2] + ".train", "/app/hadoop-2.7.3/results/NaiveBayesOutput", conf);
    	getFromHDFS(otherArgs[0] + "/" + otherArgs[4], "/app/hadoop-2.7.3/results/NaiveBayesOutput", conf);
    	
    	fs.close();
    	System.exit(0);
	}
	
	
	public static void put2HDFS(String src, String dst, Configuration conf) throws Exception
	{
		Path dstPath = new Path(dst);
		FileSystem hdfs = dstPath.getFileSystem(conf);
		
		hdfs.copyFromLocalFile(false, true, new Path(src), new Path(dst));
		
	}
	
	public static void getFromHDFS(String src, String dst, Configuration conf) throws Exception
	{
		Path dstPath = new Path(dst);
		FileSystem lfs = dstPath.getFileSystem(conf);
		String temp[] = src.split("/");
		Path ptemp = new Path(temp[temp.length-1]);
		if(lfs.exists(ptemp));
			lfs.delete(ptemp, true);
		lfs.copyToLocalFile(true, new Path(src), dstPath);
		
	}
}

