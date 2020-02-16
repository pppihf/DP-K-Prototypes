import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.math.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class Util {
	//中心点的个数
	//public static final int K=2;      //要改
	//读出中心点，注意必须包括键值
	public static List<ArrayList<String>> getCenterFile(String inputPath){
		List<ArrayList<String>> centers=new ArrayList<ArrayList<String>>();
		Configuration conf=new Configuration();
		try{
			FileSystem fs=CommonArgument.center_inputpath.getFileSystem(conf);
			Path path=new Path(inputPath);
			FSDataInputStream fsIn=fs.open(path);
			//一行一行读取参数，存在Text中，再转化为String类型
			Text lineText=new Text();
			String tmpStr=null;
			LineReader linereader=new LineReader(fsIn,conf);
			while(linereader.readLine(lineText)>0){
				ArrayList<String> oneCenter=new ArrayList<>();
				tmpStr=lineText.toString();
				//分裂String，存于容器中
				tmpStr=tmpStr.replace("\t", " ");        //要改
				tmpStr=tmpStr.replace(",", " ");        //要改
				String[] tmp=tmpStr.split(" ");
				for(int i=0;i<tmp.length;i++){
					oneCenter.add(tmp[i]);
				}
				//将此点加入集合
				centers.add(oneCenter);
			}
			fsIn.close(); 
		}catch(IOException e){
			e.printStackTrace();
		}
		//返回容器
		return centers;
	}
	
	//判断是否满足停止条件
	public static boolean isOK(String inputpath,String outputpath,int k,double threshold)throws IOException{
		//获得输入输出文件
		List<ArrayList<String>> oldcenters=Util.getCenterFile(inputpath);
		List<ArrayList<String>> newcenters=Util.getCenterFile(outputpath);
		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		System.out.println(oldcenters.get(0).size());
		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		//累加中心点间的距离
		double distance=0;
		for(int i=0;i<Canopy.K;i++){
			for(int j=1;j<KmeansReduce.dimension1 + 1;j++){
				double tmp=Math.abs(Double.parseDouble(oldcenters.get(i).get(j))-Double.parseDouble(newcenters.get(i).get(j)));
				distance+=Math.pow(tmp,2);
			}
			distance=Math.sqrt(distance);//计算欧式距离
			for(int j=KmeansReduce.dimension1 + 1;j<KmeansReduce.dimension1 + KmeansReduce.dimension2;j++){
				double tmp=Double.parseDouble(oldcenters.get(i).get(j))-Double.parseDouble(newcenters.get(i).get(j));
				if (tmp!=0){
					distance+=KPrototypesDistance.lambda;     //计算普通匹配距离
				}
			}
		}
		/*如果超出阈值，则返回false
		 * 否则更新中心点文件
		*/
		System.out.println("这次的距离"+distance);
		if(distance<threshold){
			return false;
		}
		
		//更新中心点文件
		Util.deleteLastResult(inputpath);//先删除旧文件
		Configuration conf=new Configuration();
		Path ppp=new Path("hdfs://localhost:9000/user/hadoop/");
		FileSystem fs=ppp.getFileSystem(conf);
		//通过local作为中介
		fs.moveToLocalFile(new Path(outputpath), new Path(
						"/home/hadoop/tmp/tmp.data"));
		fs.delete(new Path(inputpath), true);//在写入inputpath之前再次确保此文件不存在
		fs.moveFromLocalFile(new Path("/home/hadoop/tmp/tmp.data")
						,new Path(inputpath));
		return true;
	}
	
	//删除上一次mapreduce的结果
	public static void deleteLastResult(String inputpath){
		Configuration conf=new Configuration();
		try{
			Path ppp=new Path(inputpath);
			FileSystem fs2= ppp.getFileSystem(conf);
			fs2.delete(new Path(inputpath),true);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public static int expMachanism(int[] count,int m,double epsilon,int sensitivity){
		BigDecimal probability[]=new BigDecimal[10];
	    int j=0;
	    double expo;
	    BigDecimal exp=BigDecimal.valueOf(Math.E);
	    BigDecimal sum=BigDecimal.valueOf(0);
	    BigDecimal sum_exp=BigDecimal.valueOf(0);

	    for(int i=0;i<m;i++){
	    	if(count[i]==0){
	    		probability[i]=BigDecimal.valueOf(0);
	    	}
	    	else{
		        expo = 0.5*(double)(count[i])*epsilon/sensitivity;
		        probability[i]=exp.pow((int)expo);
	    	}
	    }
	    for(int i=0;i<m;i++){
	        sum=sum.add(probability[i]);
	    }
	    for(int i=0;i<m;i++){
	    	probability[i]=probability[i].divide(sum, 6, BigDecimal.ROUND_HALF_UP);
	    }

	    BigDecimal r=BigDecimal.valueOf(Math.random());
	    for(j=0;;j++){
	        sum_exp=sum_exp.add(probability[j]);
	        if(sum_exp.compareTo(r)==1)
	            break;
	    }
	    return j;
	}
	
	public static int DI_expMachanism(int[] count,int upper,double rho,int m,int sensitivity){
		BigDecimal probability[]=new BigDecimal[10];
	    int j=0;
	    double expo;
	    BigDecimal exp=BigDecimal.valueOf(Math.E);
	    BigDecimal sum=BigDecimal.valueOf(0);
	    BigDecimal sum_exp=BigDecimal.valueOf(0);

	    for(int i=0;i<upper;i++){
	    	if(count[i]==0){
	    		probability[i]=BigDecimal.valueOf(0);
	    	}
	    	else{
		        expo = 0.5*(double)(count[i])*Math.log(rho*m)/sensitivity;
		        probability[i]=exp.pow((int)expo);
	    	}
	    }
	    for(int i=0;i<upper;i++){
	        sum=sum.add(probability[i]);
	    }
	    for(int i=0;i<upper;i++){
	    	probability[i]=probability[i].divide(sum, 6, BigDecimal.ROUND_HALF_UP);
	    }

	    BigDecimal r=BigDecimal.valueOf(Math.random());
	    for(j=0;;j++){
	        sum_exp=sum_exp.add(probability[j]);
	        if(sum_exp.compareTo(r)==1)
	            break;
	    }
	    return j;
	}
	
	public static double maxCount(int count[],int m,double n){//参数：存放属性值个数的数组、该数组的长度、该属性的默认值
		int max=count[0];
		double j=n;
		
		for(int i=(int)j;i<m;i++){
			if(count[i]>max){  // 判断最大值
				max=count[i];
				j=i;
			}
		}
		return j;
	}
}
