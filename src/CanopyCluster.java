import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import king.Utils.Distance;
import king.Utils.EuclideanDistance;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CanopyCluster {
	public static class CanopyClusterMapper extends Mapper<LongWritable, Text, IntWritable, Instance>{
		
		public static int K =2;    //聚类个数
		private ArrayList<Cluster> canopyClusters = new ArrayList<Cluster>();
		/**
		 * 读入目前的簇信息
		 */
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			super.setup(context);
			FileSystem fs = FileSystem.get(context.getConfiguration());
	        FileStatus[] fileList = fs.listStatus(new Path(context.getConfiguration().get("canopyPath")));
	        BufferedReader in = null;
			FSDataInputStream fsi = null;
			String line = null;
	        for(int i = 0; i < fileList.length; i++){
	        	if(!fileList[i].isDirectory()){
	        		fsi = fs.open(fileList[i].getPath());
					in = new BufferedReader(new InputStreamReader(fsi,"UTF-8"));
					while((line = in.readLine()) != null){
						System.out.println("read a line:" + line);
						Cluster cluster = new Cluster(line);
						cluster.setNumOfPoints(0);
						canopyClusters.add(cluster);
					}
	        	}
	        }
	        in.close();
	        fsi.close();
	        K = canopyClusters.size();
		}
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			Instance instance = new Instance(value.toString());
			int id;
			
			try {
				Distance<Double> distanceMeasure = new KPrototypesDistance<Double>();
				double newDis = 0.0;
				for(Cluster cluster : canopyClusters){       //计算各数据与中心点之间的距离
					newDis = distanceMeasure.getDistance(cluster.getCenter().getValue(), instance.getValue());
					id = cluster.getClusterID();
					if(newDis<Canopy.T1){           //若距离小于T1，将该数据点归于对应的Canopy
						context.write(new IntWritable(id), instance);
					}
					id++;
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
