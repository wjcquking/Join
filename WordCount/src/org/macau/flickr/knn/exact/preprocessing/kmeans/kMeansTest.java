package org.macau.flickr.knn.exact.preprocessing.kmeans;

import java.util.ArrayList;
import java.util.List;

public class kMeansTest {

	public static void main(String[] args){
		
		//double[][] points = {{0, 0.1}, {4, 10}, {1, 1}, {5, 8},{10,1.1}}; //测试数据，四个二维的点
		
		List<double[]> list = new ArrayList<double[]>();
		list.add(new double[]{1,1});
		list.add(new double[]{5,8});
		list.add(new double[]{0,0});
		list.add(new double[]{4,10});
		double[][] points = list.toArray(new double[0][0]);
		
		kmeans_data data = new kmeans_data(points, 4, 2); //初始化数据结构
		kmeans_param param = new kmeans_param(); //初始化参数结构
		param.initCenterMehtod = kmeans_param.CENTER_RANDOM; //设置聚类中心点的初始化模式为随机模式
	   
		//做kmeans计算，分两类
		kmeans.doKmeans(2, data, param);
	  
		//查看每个点的所属聚类标号
		System.out.print("The labels of points is: ");
		for (int lable : data.labels) {
			System.out.print(lable + "  ");
		}
		
		System.out.println(data.centers);
		for(double[] i: data.centers){
			for(double j: i){
				System.out.print(j + "  ");
			}
			System.out.println();
		}
		
		
	}
}
