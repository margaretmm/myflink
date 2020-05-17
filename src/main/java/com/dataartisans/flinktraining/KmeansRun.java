public class KmeansRun {
    private int kNum;
    private int iterNum=10;

    private int iterMaxTimes=100000;
    private int iterRunTimes =0;
    private float disDiff =(float) 0.01;

    private List<float[]> original_data=null;
    private static List<Point> pointList=null;

    private DistantCompute disC=new DistantCompute();
    private int len=0;

    public KmeansRun(int k,List<float[]> original_data){
        this.kNum=k;
        this.original_data=original_data;
        this.len=original_data.get(0).length;
        //检查规范
        check();
        //初始化点集
        init();
    }

    public void  check(){
        if(kNum==0){
            throw new IllegalArgumentException("k must >0!");
        }
        if(original_data==null){
            throw new IllegalArgumentException("original_data==null!");
        }
    }

    //把原始数据集转换成Point结构
    private void init(){
        pointList= new ArrayList<Point>();
        for(int i=0,j=original_data.size();i<j;i++){
            pointList.add(new Point(i,original_data.get(i)));
        }
    }

    //kmeams 迭代入口
    public Set<Cluster> run(){
        Set<Cluster> clusterSet=  chooseCenterCluster();
        boolean ifNeedIter=true;
        while (ifNeedIter){
            cluster(clusterSet);
            ifNeedIter=calculateCenter(clusterSet);
            iterRunTimes++;
        }
        return clusterSet;
    }

    private Set<Cluster>  chooseCenterCluster(){
        Set<Cluster> clusterSet=new HashSet<>();
        Random random=new Random();

        for(int id=0;id<kNum;){
            //从原始数据集中随机选择要给点
            Point point =pointList.get(random.nextInt(pointList.size()));
            //用于标记是否已经选择过该数据
            boolean flag=true;
            for(Cluster cluster:clusterSet){
                if(cluster.getCenter().equals(point)){
                    flag=false;
                }
            }

            //如果随机选择的点没有被选中过，则新建一个Cluster
            if(flag){
                Cluster cluster=new Cluster(id,point);
                clusterSet.add(cluster);
                id++;
            }
        }

        return clusterSet;

    }

    //为每个点分配一个cluster
    public void cluster(Set<Cluster> clusterSet){
        //计算每个点到 所有cluster 中心的距离，并且为每个点标记cluster id
        for(Point point: pointList){
            float min_dis=Integer.MAX_VALUE;
            for(Cluster cluster:clusterSet){
                float tmp_dis=(float) Math.min(disC.getEuclideanDis(point,cluster.getCenter()),min_dis);
                if(tmp_dis != min_dis){
                    min_dis=tmp_dis;
                    point.setClusterId(cluster.getId());
                    point.setDist(min_dis);
                }
            }
        }

        //清除原来所有cluster中成员，把所有的点，分别加入每个cluster
        for(Cluster cluster:clusterSet){
            cluster.getMembers().clear();
            for(Point point:pointList){
                if(point.getClusterId()==cluster.getId()){
                    cluster.addPoint(point);
                }
            }
        }

    }

    //计算每个cluster的中心点
    public boolean calculateCenter(Set<Cluster> clusterSet) {
        boolean ifNeedIter=false;

        for(Cluster cluster:clusterSet){
            List<Point> point_List=cluster.getMembers();
            float[] sumAll=new float[len];

            //所有点，对应各个维度求和
            for(int i=0;i<len;i++){
                for(int j=0;j<point_List.size();j++){
                    sumAll[i]+=point_List.get(j).getLocalArray()[i];
                }
            }

            //计算均值
            for(int i=0;i<sumAll.length;i++){
                sumAll[i]=(float) sumAll[i]/point_List.size();
            }

            Point newPoint = new Point(sumAll);
            //计算2个新旧中心点的距离，如果任意一个cluster中心移动的距离>dis_diff就继续迭代
            if(disC.getEuclideanDis(cluster.getCenter(),newPoint)>disDiff){
                ifNeedIter = true;
            }

            cluster.setCenter(newPoint);
        }
        return ifNeedIter;
    }

}
