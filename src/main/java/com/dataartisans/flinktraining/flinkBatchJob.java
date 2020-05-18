package com.demo.flink.batch.ML.Kmeans;


import com.demo.flink.batch.ML.Logistic.LogicInfo;
import com.demo.flink.batch.ML.Logistic.LogicMap;
import com.demo.flink.batch.ML.Logistic.LogicRegressionReduce;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.GroupReduceOperator;

import java.util.*;

/**
 * Implements the Oylympics Athletes program that gives insights about games played and medals won. 
 * 
 * Sample input file is provided in src/main/resources/data folder
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink batch program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class flinkBatchJob {
	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		ArrayList<float[]> dataSet= new ArrayList<float[]>();

		dataSet.add(new float[]{1,2,3});
		dataSet.add(new float[]{3,3,3});
		dataSet.add(new float[]{3,4,4});
		dataSet.add(new float[]{6,6,5});
		dataSet.add(new float[]{3,9,6});
		dataSet.add(new float[]{4,5,4});
		dataSet.add(new float[]{3,9,7});
		dataSet.add(new float[]{5,9,8});
		dataSet.add(new float[]{4,2,8});
		dataSet.add(new float[]{5,5,8});
		dataSet.add(new float[]{6,5,8});
		dataSet.add(new float[]{1,1,1});

		DataSet<String> dataInput = env.readCsvFile("olympic-athletes.csv")
				.pojoType(String.class, "playerName", "country", "year", "game", "gold", "silver", "bronze", "total");
		DataSet<KmeansInputData> kmeansInputDataSet = dataInput
				.map(new KmeansMap());

		DataSet<ArrayList<Point>> clusterCentersDataSet = kmeansInputDataSet.groupBy("groupbyId").reduceGroup(new KmeansReduce());
		kmeansInputDataSet.print();

		try{
			List<ArrayList<Point>>  retList=clusterCentersDataSet.collect();
            ArrayList<float[]> dataSet=new ArrayList<float[]>();

			for(ArrayList<Point> arr: retList){
				for (Point point:arr){
					dataSet.add(point.getLocalArray());
				}
			}

			KmeansRun kRun = new KmeansRun(3,dataSet);
			Set<Cluster> clusterSet=kRun.run();
			List<Point> finalClusterCenter= new ArrayList<Point>();
			int count=100;
			for(Cluster cluster:clusterSet){
				Point point =cluster.getCenter();
				point.setId(count++);
				finalClusterCenter.add(point);
			}

			//标识输入point类型数据具体是归属于哪个cluster
			DataSet<Point> finalMap = dataInput.map(new KmeansLabelMap(finalClusterCenter));
			//finalMap.writeAsText("/xxx");
			finalMap.print();

			env.execute("kmeans predict");
		}catch (Exception e){
			e.printStackTrace();
		}
	}
}
