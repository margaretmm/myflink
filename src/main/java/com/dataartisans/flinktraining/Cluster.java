public class Cluster {

    private int id;
    private Point center;
    private List<Point> members= new ArrayList<>();
    private float dist;

    public Cluster(int id,Point center){
        this.id=id;
        this.center=center;
    }
    public Cluster(int id,Point center,List<Point> members){
        this.id=id;
        this.center=center;
        this.members=members;
    }

    public void addPoint(Point newPoint){
        if(! members.contains(newPoint)){
            members.add(newPoint);
        }else{
            System.out.println("Point 已经存在:"+newPoint.toString());
        }
    }
    @Override
    public String toString(){
        String toString="Cluster \n"+"Cluster_id="+this.id+
                ",center:{"+this.center.toString()+"}";

        for(Point  point:members){
            toString+="\n"+point.toString();
        }
        return toString+"\n";
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Point getCenter() {
        return center;
    }

    public void setCenter(Point center) {
        this.center = center;
    }

    public List<Point> getMembers() {
        return members;
    }

    public void setMembers(List<Point> members) {
        this.members = members;
    }

    public float getDist() {
        return dist;
    }

    public void setDist(float dist) {
        this.dist = dist;
    }

}
