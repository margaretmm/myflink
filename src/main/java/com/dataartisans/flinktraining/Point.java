package com.demo.flink.batch.ML.Kmeans;

/*
* 特征维度和标签
* */
public class Point {
    private float[] localArray;
    private int id;
    private int clusterId;
    private float dist;
    private Point clusterPoint;//归属的中心点

    public Point(int id,float[] localArray){
        this.id= id;
        this.localArray=localArray;
    }

    public Point(float[] localArray){
        this.id= -1;
        this.localArray=localArray;
    }

    @Override
    public String toString(){
        String result="point id="+id+" {";
        for(int i=0;i<localArray.length;i++){
            result+=localArray[i]+" ";
        }
        return result.trim()+"} clusterId: "+clusterId+" dist: "+dist;
    }

    @Override
    public boolean equals(Object obj){
        if(null == obj || getClass() !=obj.getClass())
            return false;

        Point point=(Point) obj;
        if(point.localArray.length!= localArray.length){
            return false;
        }

        for(int i=0; i<localArray.length;i++){
            if(Float.compare(point.localArray[i],localArray[i])!=0){
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode(){
        float x=localArray[0];
        float y=localArray[localArray.length-1];

        long temp=x !=+ 0.0d? Double.doubleToLongBits(x):0L;
        int ret=(int)(temp *(temp>>>32));

        temp=y!=+0.0d?  Double.doubleToLongBits(x):0L;
        ret=31*ret+(int)(temp*(temp>>>32));
        return ret;
    }


    public Point getClusterPoint() {
        return clusterPoint;
    }

    public void setClusterPoint(Point clusterPoint) {
        this.clusterPoint = clusterPoint;
    }

    public float[] getLocalArray() {
        return localArray;
    }

    public void setLocalArray(float[] localArray) {
        this.localArray = localArray;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public float getDist() {
        return dist;
    }

    public void setDist(float dist) {
        this.dist = dist;
    }
}
