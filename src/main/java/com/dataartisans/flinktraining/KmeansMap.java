public class KmeansMap implements MapFunction<String,KmeansInputData> {

    @Override
    public KmeansInputData map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        Random random =new Random();
        String []  temps=s.split(",");
        String v1=temps[0];
        String v2=temps[1];
        String v3=temps[2];

        KmeansInputData kmeansInputData=new KmeansInputData();
        kmeansInputData.setV1(v1);
        kmeansInputData.setV2(v2);
        kmeansInputData.setV3(v3);
        kmeansInputData.setGroupbyId("kmeans=="+random.nextInt(10));
        return kmeansInputData;
    }
}
