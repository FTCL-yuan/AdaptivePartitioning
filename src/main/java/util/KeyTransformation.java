package util;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class KeyTransformation <K>{
    public static byte[] hashCodeGet(Object key){
        ByteBuffer out = ByteBuffer.allocate(4);
        //key = "test";
        if (key != null) {
            if (key instanceof List) {
                out.putInt(Arrays.deepHashCode(((List)key).toArray()));
            } else if (key instanceof Object[]) {
                out.putInt(Arrays.deepHashCode((Object[])key));
            } else if (key instanceof byte[]) {
                out.putInt(Arrays.hashCode((byte[]) key));
            } else if (key instanceof short[]) {
                out.putInt(Arrays.hashCode((short[]) key));
            } else if (key instanceof int[]) {
                out.putInt(Arrays.hashCode((int[]) key));
            } else if (key instanceof long[]) {
                out.putInt(Arrays.hashCode((long[]) key));
            } else if (key instanceof char[]) {
                out.putInt(Arrays.hashCode((char[]) key));
            } else if (key instanceof float[]) {
                out.putInt(Arrays.hashCode((float[]) key));
            } else if (key instanceof double[]) {
                out.putInt(Arrays.hashCode((double[]) key));
            } else if (key instanceof boolean[]) {
                out.putInt(Arrays.hashCode((boolean[]) key));
            } else {
                //System.out.println(Thread.currentThread().getId()+":"+key+"::"+key.hashCode());
                //out.clear();
                out.putInt(key.hashCode());
                //System.out.println(Thread.currentThread().getId()+":"+key+"::"+out.array()+":::"+out.array().length);

            }

        } else {
            out.putInt(0);
        }
        //System.out.println(Thread.currentThread().getId()+":"+key+"::"+out.array()+":::"+out.array().length);
        return out.array();
    }

    public static int selectMinChoice(double loadVector[], int choice[]) {
        int index = choice[0];
        for(int i = 0; i< choice.length; i++) {
            if (loadVector[choice[i]]<loadVector[index]) {
                index = choice[i];
            }
        }
        return index;
    }
}
