package Util;

/**
 * @author yshhuang@foxmail.com
 * @date 2019-11-19 15:40
 */
public class MyUtils {

    public static String doubleToBinaryString(double... doubles) {
        StringBuilder sb=new StringBuilder();
        for (int i = 0; i < doubles.length; i++) {
            byte b = (byte) doubles[i];
            String s = String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
            sb.append(s);
        }
        return sb.toString();
    }

    public static void main(String[] args){
        String s=doubleToBinaryString(new double[]{125.0,4.0});
        System.out.println(s);
    }
}
