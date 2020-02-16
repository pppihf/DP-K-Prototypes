
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;

public class KmeansReduce extends Reducer<Text, Text, Text,Text> {
	public static final int dimension1 = 6;// 定义数值属性维度
	public static final int dimension2 = 7;// 定义非数值属性维度
	public static final int upper = 8;//最大的非数值属性值加1
	
	public void reduce(Text key,Iterable<Text> value,Context context)throws IOException,InterruptedException{
		//把value值放进String数组中（只获取和处理一个），value是Mapper输出的记录属性向量集
		long num=0;

		double[] num_para = new double[dimension1];
		int count[][] = new int[dimension2][upper];
		double categorical[] = {0, 1, 0, 0, 0, 1, 3};//默认值
		
		for(Text T:value){
			num++;
			String onePoint=T.toString();
			onePoint=onePoint.replace("\t", " ");     //要改
			String[] parameters=onePoint.split(" ");  //以空格为分割标志分割该条数据

			String[] cate_para=new String[dimension2];
			for(int i=0;i<dimension2;i++){
				cate_para[i]=parameters[i+dimension1];
			}
			
			//进行累加
			for(int i=0;i<dimension1;i++){
				num_para[i]+=Double.parseDouble(parameters[i]);  //计算每个数值型属性的和
			}
			
			for(int i=0;i<dimension2;i++){
				for(int j=0;j<upper;j++){
					if (Double.parseDouble(cate_para[i])==(double)j){
						count[i][j]++;                //计算非数值属性出现的次数
					}
				}
			}
		}
		
		//未加差分隐私保护
		//数值属性部分
//		double avg_num_para[] = new double[dimension1];
//		for(int i=0;i<dimension1;i++){
//			avg_num_para[i]=num_para[i]/((double)num);    //计算数值型属性的均值
//		}

		//未加差分隐私保护
		//非数值属性部分
//		for(int i=0;i<dimension2;i++){
//			categorical[i]=Util.maxCount(count[i], count[i].length, categorical[i]);   //判断非数值属性中个数最多的属性值
//		}
//		
//		System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
//		for(int i=0;i<dimension1;i++){
//			System.out.println(avg_num_para[i]);       //打印数值型属性的均值
//		}
//
//		for (int i=0;i<categorical.length;i++){
//			System.out.println(categorical[i]);
//		}
//		System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
//		String result=new String();
//		for(int i=0;i<dimension1;i++){
//			result=result+avg_num_para[i]+" ";
//		}
//		for(int i=0;i<dimension2 - 1;i++){
//			result=result+categorical[i]+" ";
//		}
//		result=result+categorical[dimension2 - 1];
		
		//差分隐私保护
		//数值属性部分
//		int deltaF=dimension1;
//		double epsilon=CommonArgument.epsilon;
//		double pri_num_para[] = new double[dimension1];
//		double pri_avg_num_para[] = new double[dimension1];
//		
//		//double pri_num=num + Laplace.pdf(deltaF*Math.pow(2.0,KmeansDriver.repeats+1)/epsilon);
//		//double pri_num=num + Laplace.pdf(deltaF/epsilon);        //添加Laplace噪声
//		for(int i=0;i<dimension1;i++){
//			pri_num_para[i]=num_para[i] + Laplace.pdf(deltaF/epsilon);
//		}
//		for(int i=0;i<dimension1;i++){
//			pri_avg_num_para[i]=pri_num_para[i]/num;       //计算加噪声后的均值
//		}
		
		//差分隐私保护
		//非数值属性部分
//		for(int i=0;i<dimension2;i++){
//			categorical[i]=Util.expMachanism(count[i], upper, epsilon, 1);
//		}
//		
//		System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
//		for(int i=0;i<dimension1;i++){
//			System.out.println(pri_avg_num_para[i]);        //打印加噪声后数值属性的均值
//		}
//		for (int i=0;i<categorical.length;i++){
//			System.out.println(categorical[i]);
//		}
//		System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
//		String result=new String();
//		for(int i=0;i<dimension1;i++){
//			result=result+pri_avg_num_para[i]+" ";
//		}
//		for(int i=0;i<dimension2 - 1;i++){
//			result=result+categorical[i]+" ";
//		}
//		result=result+categorical[dimension2 - 1];
		
		//差分可辨性
		//数值属性部分
		int deltaF = dimension1;
		int m1 = 10;
		double rho1[] = {0.55,0.24,0.2291};  //分别是rho，rho1，rho2
		double pri_num_para[] = new double[dimension1];
		double pri_avg_num_para[] = new double[dimension1];
		double nowRho = rho1[KmeansDriver.repeats+1];       //当前迭代轮次的rho
		System.out.println(Math.log((m1-1)*nowRho/(1-nowRho)));
		for(int i=0;i<dimension1;i++){
			pri_num_para[i]=num_para[i] + Laplace.pdf(deltaF/(Math.log((m1-1)*nowRho/(1-nowRho))));
		}
		for(int i=0;i<dimension1;i++){
			pri_avg_num_para[i]=pri_num_para[i]/num;       //计算加噪声后的均值
		}
		
		//差分可辨性
		//非数值属性部分
		int m2 = 2;
		nowRho = rho1[0]; 
		for(int i=0;i<dimension2;i++){
			categorical[i]=Util.DI_expMachanism(count[i], upper, nowRho, m2, 1);
		}
		
		System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		for(int i=0;i<dimension1;i++){
			System.out.println(pri_avg_num_para[i]);        //打印加噪声后数值属性的均值
		}
		for (int i=0;i<categorical.length;i++){
			System.out.println(categorical[i]);
		}
		System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		String result=new String();
		for(int i=0;i<dimension1;i++){
			result=result+pri_avg_num_para[i]+" ";
		}
		for(int i=0;i<dimension2 - 1;i++){
			result=result+categorical[i]+" ";
		}
		result=result+categorical[dimension2 - 1];
		
		context.write(key,new Text(result));
	}
}
