/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.extensions;
import java.util.Comparator;
import xklusac.environment.ResourceInfo;

/**
 *
 * @author Aairah
 */
public class CostComparator implements Comparator{
    /**
     * Compares two resources according to their estimated length
     */
    public int compare(Object o1, Object o2) {
        ResourceInfo g1 = (ResourceInfo) o1;
        ResourceInfo g2 = (ResourceInfo) o2;
        double length1 = (Double) g1.resource.getCostPerSec();
        double length2 = (Double) g2.resource.getCostPerSec();;
        if(length1 > length2) return 1;
        if(length1 == length2) return 0;
        if(length1 < length2) return -1;
        return 0;
    }
}
