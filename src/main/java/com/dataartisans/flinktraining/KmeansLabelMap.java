import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class KmeansLabelMap implements MapFunction<String,Point> {

    private List<Point> centers=new ArrayList<Point>();
    private DistantCompute disCpt=new DistantCompute();

    public KmeansLabelMap(List<Point> centers){
        this.centers=centers;
    }

    @Override
    public Point map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        Random random =new Random();
        String []  temps=s.split(",");
        String v1=temps[0];
        String v2=temps[1];
        String v3=temps[2];
        Point self= new Point(1,new float[]{
                Float.valueOf(v1), Float.valueOf(v2), Float.valueOf(v3)
        });
        float min_dis=Integer.MAX_VALUE;
        for(Point point:centers){
            float tmp_dis=(float) Math.min(disCpt.getEuclideanDis(self,point),min_dis);
            if(tmp_dis != min_dis){
                min_dis=tmp_dis;
                self.setClusterId(point.getId());
                point.setDist(min_dis);
            }
        }

        return self;
    }
}
