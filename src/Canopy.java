import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;


import java.util.*;

import king.Utils.Distance;
import king.Utils.EuclideanDistance;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.LineReader;

public class Canopy {
	static double T1;
	public static int K =2;    //聚类个数
	
	
	public static class CanopyMapper extends Mapper<LongWritable,Text,IntWritable,Cluster>{
		private ArrayList<Cluster> canClusters = new ArrayList<Cluster>();
	
		@Override
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			Instance instance = new Instance(value.toString());     //将读取的数据存为Instance
			Cluster cluster0 = new Cluster(1,instance);        //该数据对象作为类中心
			canClusters.add(cluster0);                //将数据存放到canClusters中
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException{
			int id=1;
			ArrayList<Cluster> canQ = new ArrayList<Cluster>();
			try {
				int L = canClusters.size();     //L为map函数中局部数据集大小
				for(int i=0;i<Math.sqrt(L);i++){
					if(i == 0){                  //若Q为空，求数据集中xi与原点距离最小值，将该点保存至Q
						int index = getNearest(canClusters);
						Cluster cluster = canClusters.get(index);
						
						canQ.add(cluster);
						context.write(new IntWritable(id),cluster);
					}
					else{                  //若Q不空，求数据集中xi与Q中数据点的最小距离中的最大者，将该点保存至Q
						int index1 = getMax(canClusters , canQ);
						Cluster cluster1 = canClusters.get(index1);
						
						canQ.add(cluster1);
						context.write(new IntWritable(id),cluster1);
					}
				}
				System.out.println("canQsize:" + canQ.size());  //打印Q中点的个数
			}catch(Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public  int getNearest(ArrayList<Cluster> set) throws Exception{
			Instance instance= new Instance(KmeansReduce.dimension1);    //括号里面应是数值型属性维数
			double distance = Double.MAX_VALUE;
			Distance<Double> distanceMeasure = new KPrototypesDistance<Double>();
			double newDis = 0.0;
			int index = -1;
			for(int j=0;j<set.size();j++){    //求数据集中xi原点距离的最小值
				newDis = distanceMeasure.getDistance(set.get(j).getCenter().getValue(), instance.getValue());
				if(newDis < distance){
					 index = j;
					 distance = newDis;
				}
			}
			return index;
		}
		
		public int getMax(ArrayList<Cluster> set, ArrayList<Cluster>Qset) throws Exception{
			int index = -1;
			ArrayList<Double>dislist = new ArrayList<Double>();
			double dis = 0.0;
			double distance = 0.0;
			Distance<Double> distanceMeasure = new KPrototypesDistance<Double>();
			if(Qset.size()==1){             //若Q中只有一个点
				for(int i=0;i<set.size();i++){
					double dist = distanceMeasure.getDistance(set.get(i).getCenter().getValue(),
							Qset.get(0).getCenter().getValue());
					if(dist>distance){
						distance=dist;
						index = i;
					}
				}
				return index;
			}
			else{              //若Q中至少有2个点
				for(int i = 0;i<set.size();i++){
					double Distance = Double.MAX_VALUE;
					for(int j = 0;j<Qset.size();j++){     //求数据集中某个点与Q中点的最小距离
						double Dis = distanceMeasure.getDistance(set.get(i).getCenter().getValue(),
								Qset.get(j).getCenter().getValue());
						if(Dis == 0){
							Distance = 0.0;
							break;
						}
						else{
							if(Dis<Distance){
								Distance = Dis;
							}
						}	
					}
					dislist.add(Distance);     //将最小距离组成数组
				}
				
				for(int i=0;i<dislist.size();i++){   //求最小距离中的最大者
					if(dislist.get(i)>dis){
						dis = dislist.get(i);
						index=i;
					}
				}
				return index;
			}
		}
	} 
	
	public  static class CanopyReducer extends Reducer<IntWritable,Cluster,IntWritable,Instance>{
		public void reduce(IntWritable key, Iterable<Cluster> values, Context context)throws IOException, InterruptedException{
			ArrayList<Cluster> set1 = new ArrayList<Cluster>();
			
			for(Cluster clusters : values){             //将集合Q复制到set1中
				Cluster cluster = WritableUtils.clone(clusters, context.getConfiguration());
				set1.add(cluster);
			}
			
		    ArrayList<Cluster> Qset1 = new ArrayList<Cluster>();
		    List<ArrayList<Double>> Uset = new ArrayList<ArrayList<Double>>();
			ArrayList<Double> distancelist = new ArrayList<Double>();
			ArrayList<Double> depth = new ArrayList<Double>();
			double distance = 0.0;
	
			try{
				int index= -1;
				/**
				 * 计算canopy的集合
				 */
				int L = set1.size();    //L为集合Q的数据总量
				
				for(int i=0;i<Math.sqrt(L);i++){       //计算集合Q中数据点之间距离最小值中最大者
					if(i == 0){
						Cluster cluster0 = getNearest(set1);
						System.out.println("cluster0:" + cluster0.toString());   //打印结果应是：距离最小的点在Q中的序号,0,最小的距离
						for(int j =0;j<cluster0.getCenter().getValue().size();j++){
							distancelist.add(cluster0.getCenter().getValue().get(j));
						}
					    Cluster cluster = set1.get(cluster0.getClusterID());
					   
					    Qset1.add(cluster);     //将点加入集合Q'中
					}
					else{
						Cluster cluster1 = getMax(set1,Qset1);
						System.out.println("cluster1:" + cluster1.toString());     //打印结果应是：最小距离中最大的点在Q中的序号,0,距离
						for(int j =0;j<cluster1.getCenter().getValue().size();j++){
							distancelist.add(cluster1.getCenter().getValue().get(j));
						}
						
						Cluster cluster2 = set1.get(cluster1.getClusterID());        //将点加入集合Q'中
						Qset1.add(cluster2);
					}
				}
	
               /**
                * 计算depth
                */
				for(int i =1;i<distancelist.size()-1;i++){
				    depth.add( Math.abs(distancelist.get(i)-distancelist.get(i-1))+
				    		 Math.abs(distancelist.get(i+1)-distancelist.get(i)));
				}
				
				for(int i =0;i<depth.size();i++){    //计算depth的最大值，注意序号
					if(depth.get(i)> distance){
						distance = depth.get(i);
						index = i;
					}
				}
				Canopy.T1 = distancelist.get(index+1);
				
				for(int i =0;i<index+2;i++){
					Uset.add(Qset1.get(i).getCenter().getValue());     //将Q'中前i个点赋值至空集合U中
				}
				K = Uset.size();
				for(int i =0;i<Uset.size();i++){
					Instance instance =new Instance();
					instance.value = Uset.get(i);
					Cluster cluster = new Cluster(i,instance);
					context.write(new IntWritable(cluster.getClusterID()), instance);      //Reducer输出集合U
				}
			}catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
       public  Cluster getNearest(ArrayList<Cluster> set) throws Exception{
			Instance instance1= new Instance(KmeansReduce.dimension1);      //括号里应该是数值型属性维数
			double distance = Double.MAX_VALUE;
			Distance<Double> distanceMeasure = new KPrototypesDistance<Double>();
			double newDis = 0.0;
			int index = -1;
			for(int j=0;j<set.size();j++){
				newDis = distanceMeasure.getDistance(set.get(j).getCenter().getValue(), instance1.getValue());
				if(newDis < distance){
					 index = j;
					 distance = newDis;
				}
			}
			ArrayList<Double> mindis =new ArrayList<Double>();
			mindis.add(distance);
			Instance instance =new Instance();
			instance.value = mindis;
			Cluster cluster = new Cluster(index,instance);
			return cluster;
		}
		
		public Cluster getMax(ArrayList<Cluster>set,ArrayList<Cluster>Qset)	throws Exception{
			int index = -1;
			double dis = 0.0;
			double distance = 0.0;
			Distance<Double> distanceMeasure = new KPrototypesDistance<Double>();
			if(Qset.size()==1){              //若Q'中只有1个数据点
				ArrayList<Double> dislist0 = new ArrayList<Double>();
				for(int i=0;i<set.size();i++){
					double dist = distanceMeasure.getDistance(set.get(i).getCenter().getValue(),
							Qset.get(0).getCenter().getValue());
					if(dist>distance){
						distance=dist;
						index = i;
					}
				}
				dislist0.add(distance);
				Instance instance0 =new Instance();
				instance0.value = dislist0;
				Cluster cluster0 = new Cluster(index,instance0);
				return cluster0;
			}
			else{          //若Q'中至少有2个数据点
				ArrayList<Double> dislist = new ArrayList<Double>();
				for(int i = 0;i<set.size();i++){
					double Distance = Double.MAX_VALUE;
					for(int j = 0;j<Qset.size();j++){           //求Q中某个点与Q'中点的最小距离
						double Dis = distanceMeasure.getDistance(set.get(i).getCenter().getValue(),
								Qset.get(j).getCenter().getValue());
						if(Dis == 0){
							Distance = 0.0;
							break;
						}	
						else{
							if(Dis<Distance){
								Distance = Dis;
							}
						}
					}
					dislist.add(Distance);                //将最小距离组成数组
				}
				
				for(int i=0;i<dislist.size();i++){          //求最小距离中最大者
					if(dislist.get(i)>dis){
						dis = dislist.get(i);
						index=i;
					}
				}
				ArrayList<Double> maxdis = new ArrayList<Double>();
				maxdis.add(dis);
				Instance instance = new Instance();
				instance.value = maxdis;
				Cluster cluster = new Cluster(index,instance);
				return cluster;
			}
		}
	}
}
