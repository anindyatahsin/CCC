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
    public String institute;
    public CostComparator(String ins){
        institute = ins;
    }
    public int compare(Object o1, Object o2) {
        ResourceInfo g1 = (ResourceInfo) o1;
        ResourceInfo g2 = (ResourceInfo) o2;
        double inst1 = 0;
        double inst2 = 0;
        if(institute != null){
            inst1 = (institute.equals(g1.getInstitute()))? 0:0.5;
            inst2 = (institute.equals(g2.getInstitute()))? 0:0.5;
        }
        double length1 = (Double) g1.resource.getCostPerSec() + inst1;
        double length2 = (Double) g2.resource.getCostPerSec() + inst2;
        if(length1 > length2) return 1;
        if(length1 == length2) return 0;
        if(length1 < length2) return -1;
        return 0;
    }
}
