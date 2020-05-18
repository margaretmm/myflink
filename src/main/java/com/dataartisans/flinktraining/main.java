package com.demo.flink.batch.ML.Kmeans;

import org.apache.flink.api.java.DataSet;

import java.util.ArrayList;
import java.util.Set;

public class main {

    public static void main(String[] args){
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

        KmeansRun kRun= new KmeansRun(3, dataSet);
        Set<Cluster> clusterSet =kRun.run();
        for(Cluster cluster:clusterSet){
            System.out.println(cluster);
        }
    }
}
