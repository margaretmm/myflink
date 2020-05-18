public class KmeansReduce implements GroupReduceFunction<KmeansInputData,ArrayList<Point>> {

    @Override
    public void reduce(Iterable<KmeansInputData> iterable, Collector<ArrayList<Point>> collector) throws Exception {
        Iterator<KmeansInputData> iterator = iterable.iterator();
        ArrayList<float[]> dataSet=new ArrayList<float[]>();

        while(iterator.hasNext()){
            KmeansInputData kmeansInputData= iterator.next();

            float[] f=new float[]{
                Float.valueOf(kmeansInputData.getV1()), Float.valueOf(kmeansInputData.getV2()), Float.valueOf(kmeansInputData.getV3())
            };
            dataSet.add(f);
        }

        KmeansRun kmeansRun = new KmeansRun(3,dataSet);

        Set<Cluster> clusterSet= kmeansRun.run();
        ArrayList<Point> centers=new ArrayList<Point>();
        for(Cluster cluster:clusterSet){
            centers.add(cluster.getCenter());
        }
        collector.collect(centers);
    }
}
