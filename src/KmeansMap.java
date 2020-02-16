import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansMap extends Mapper<LongWritable, Text, Text, Text> {
	public static final int dimension1 = 6;// 定义数值属性维度
	public static final int dimension2 = 7;// 定义非数值属性维度
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 首先将value中的数字提取出来，放到容器parameter当中
		String data = value.toString();
		data=data.replace("\t", " ");           //要改
		String[] tmpSplit = data.split(" ");
		System.out.println("#########################################################");
		System.out.println(tmpSplit.length);    //打印属性的个数即parameter中的元素个数
		System.out.println("#########################################################");
		List<String> parameter = new ArrayList<>();
		for (int i = 0; i < tmpSplit.length; i++) {
			parameter.add(tmpSplit[i]);
		}
		// 读取中心点文件(其中路径参数是从命令中获取的)
		List<ArrayList<String>> centers = Util.getCenterFile(CommonArgument.center_inputlocation);
		// 计算目标对象到各个中心点的距离，找最大距离对应的中心点，则认为此对象归到该点中
		String outKey="" ;// 默认聚类中心为0号中心点
		double minDist = Double.MAX_VALUE;
		System.out.println("**********************************************************");
		System.out.println(centers.size());         //打印中心的个数
		System.out.println("**********************************************************");
		for (int i = 0; i < centers.size(); i++) {      //i是中心标号
			// 由于是二维数据，因此要累加两次
			double dist = 0;
			for (int j =0;j<dimension1;j++){
				double a=Double.parseDouble(parameter.get(j));     //a是目标对象的属性值，括号里的数字是数值型属性
				double b=Double.parseDouble(centers.get(i).get(j+1));
				dist+=Math.pow(a-b,2);      //计算平方欧式距离
			}
			dist=Math.sqrt(dist);
			for(int j=dimension1;j<dimension1+dimension2;j++){
				double a=Double.parseDouble(parameter.get(j));     //a是目标对象的属性值
				double b=Double.parseDouble(centers.get(i).get(j+1));
				if ((a-b)!=0){
					dist+=KPrototypesDistance.lambda;      //计算普通匹配距离
				}
			}
			
			if (dist < minDist) {
				outKey = centers.get(i).get(0);// 中心点文件中所写的标号
				minDist = dist;
			}
			System.out.println("");
			System.out.println(dist);
			System.out.println("");
		}
		System.out.println("----------------------------------------");
		System.out.println(outKey+"+");
		System.out.println("----------------------------------------");
		context.write(new Text(outKey), value);
	}
}
