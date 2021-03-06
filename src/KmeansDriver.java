import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class KmeansDriver {
	public static int repeats = 0;
	// 主函数
	public static void main(String[] args) throws Exception {
		
		//Canopy算法
		Configuration C_conf = new Configuration();
		Job canopyJob = new Job(C_conf);
		canopyJob.setJarByClass(Canopy.class);
		FileInputFormat.addInputPath(canopyJob, CommonArgument.inputpath);
		FileSystem fs1 = CommonArgument.canopypath.getFileSystem(C_conf);
		if (fs1.exists(CommonArgument.canopypath)) {
			fs1.delete(CommonArgument.canopypath, true);
		}
		FileOutputFormat.setOutputPath(canopyJob, CommonArgument.canopypath);
		canopyJob.setMapperClass(Canopy.CanopyMapper.class);
		canopyJob.setMapOutputKeyClass(IntWritable.class);
		canopyJob.setMapOutputValueClass(Cluster.class);
		canopyJob.setReducerClass(Canopy.CanopyReducer.class);
		canopyJob.setOutputKeyClass(IntWritable.class);
		canopyJob.setOutputValueClass(Instance.class);
		canopyJob.waitForCompletion(true);
		System.out.println("canopy1 finished!");
		
//		Configuration CC_conf = new Configuration();
//		Job canopyClusterJob = new Job(CC_conf);
//		canopyClusterJob.setJarByClass(CanopyCluster.class);
//		canopyClusterJob.getConfiguration().set("canopyPath", CommonArgument.canopylocation);
//		FileInputFormat.addInputPath(canopyClusterJob, CommonArgument.inputpath);// 设置输入路径(指的是文件的输入路径)
//		FileSystem fs2 = CommonArgument.outputpath.getFileSystem(CC_conf);// 设置输出路径（指的是中心点的输出路径）
//		if (fs2.exists(CommonArgument.outputpath)) {
//			fs2.delete(CommonArgument.outputpath, true);
//		}
//		FileOutputFormat.setOutputPath(canopyClusterJob, CommonArgument.outputpath);
//		canopyClusterJob.setMapperClass(CanopyCluster.CanopyClusterMapper.class);
//		canopyClusterJob.setMapOutputKeyClass(Text.class);
//		canopyClusterJob.setMapOutputValueClass(IntWritable.class);
//		canopyClusterJob.setNumReduceTasks(0);
//		canopyClusterJob.waitForCompletion(true);
//		System.out.println("canopy2 finished!");
		
		do {
			Configuration conf = new Configuration();     //这句写上就对了
			// 新建MapReduce作业并指定作业启动类
			Job job = new Job(conf);
			job.setJarByClass(KmeansDriver.class);
			// 设置输入输出路径（输出路径需要额外加判断）
			FileInputFormat.addInputPath(job, CommonArgument.inputpath);// 设置输入路径(指的是文件的输入路径)
			FileSystem fs = CommonArgument.center_outputpath.getFileSystem(conf);
			if (fs.exists(CommonArgument.center_outputpath)) {// 设置输出路径（指的是中心点的输出路径）
				fs.delete(CommonArgument.center_outputpath, true);
			}
			FileOutputFormat.setOutputPath(job, CommonArgument.center_outputpath);
			// 为作业设置map和reduce所在类
			job.setMapperClass(KmeansMap.class);
			job.setReducerClass(KmeansReduce.class);
			// 设置输出键和值的类
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			// 启动作业
			job.waitForCompletion(true);
			repeats++;
			System.out.println("第"+repeats+"次");
		} while (repeats < CommonArgument.REPEAT && (Util.isOK(CommonArgument.center_inputlocation,
				CommonArgument.new_center_outputlocation, CommonArgument.REPEAT, CommonArgument.threshold)));
		
		// 进行最后的聚类工作（由map来完成）
		Configuration c_conf = new Configuration();
		// 新建MapReduce作业并指定作业启动类
		Job c_job = new Job(c_conf);
		// 设置输入输出路径（输出路径需要额外加判断）
		FileInputFormat.addInputPath(c_job, CommonArgument.inputpath);// 设置输入路径(指的是文件的输入路径)
		FileSystem fs = CommonArgument.outputpath.getFileSystem(c_conf);// 设置输出路径（指的是中心点的输出路径）
		if (fs.exists(CommonArgument.outputpath)) {
			fs.delete(CommonArgument.outputpath, true);
		}
		FileOutputFormat.setOutputPath(c_job, CommonArgument.outputpath);
		// 为作业设置map(没有reducer，则看到的输出结果为mapper的输出)
		c_job.setMapperClass(KmeansMap.class);
		// 设置输出键和值的类
		c_job.setOutputKeyClass(Text.class);
		c_job.setOutputValueClass(Text.class);
		// 启动作业
		c_job.waitForCompletion(true);
	}
}
