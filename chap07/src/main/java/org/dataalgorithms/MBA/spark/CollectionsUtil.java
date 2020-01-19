package org.dataalgorithms.MBA.spark;

import java.util.ArrayList;
import java.util.List;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-17
 */
public class CollectionsUtil {

    public  static <T extends Comparable<? super T>> List<T> removeOneItem(List<T> elements, int i){
        if ((elements == null) || (elements.isEmpty())) {
            return elements;
        }
        //
        if ((i < 0) || (i > (elements.size() - 1))) {
            return elements;
        }
        List<T> cloned = new ArrayList<T>(elements);
        cloned.remove(i);
        return cloned;
    }

}
