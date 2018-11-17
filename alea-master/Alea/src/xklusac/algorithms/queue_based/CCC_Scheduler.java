/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.algorithms.queue_based;

import alea.core.AleaSimTags;
import gridsim.GridSim;
import static gridsim.GridSim.clock;
import gridsim.GridSimTags;
import gridsim.Gridlet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import xklusac.algorithms.SchedulingPolicy;
import xklusac.environment.ComplexGridResource;
import xklusac.environment.ComplexGridlet;
import xklusac.environment.ExperimentSetup;
import xklusac.environment.GridletInfo;
import xklusac.environment.ResourceInfo;
import xklusac.environment.Scheduler;
import xklusac.extensions.CostComparator;
import xklusac.extensions.WallclockComparator;

/**
 *
 * @author Aairah
 */
public class CCC_Scheduler implements SchedulingPolicy{
    private Scheduler scheduler;

    public CCC_Scheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override    
    public void addNewJob(GridletInfo gi, int priority) {
        double runtime1 = new Date().getTime();
        
        if(priority == 1) {
            Scheduler.low_q.add(gi);
            Scheduler.runtime += (new Date().getTime() - runtime1);
            return;
        } else if(priority == 2) {
            Scheduler.regular_q.add(gi);
            Scheduler.runtime += (new Date().getTime() - runtime1);
            return;
        } else if(priority == 3) {
            Scheduler.high_q.add(gi);
            Scheduler.runtime += (new Date().getTime() - runtime1);
            //scheduler.sim_schedule(scheduler.getEntityId(scheduler.getEntityName()),GridSimTags.SCHEDULE_NOW, 0);
            scheduler.sim_schedule(GridSim.getEntityId("Alea_3.0_scheduler"), 0.0, AleaSimTags.EVENT_SCHEDULE, gi.getGridlet());
            return;    
        }
    }
    
    @Override
    public void addNewJob(GridletInfo gi) {
        
        double runtime1 = new Date().getTime();
        //System.out.println("New job has been received by PBS PRO");
        if(gi.getGridlet().getInst().equals("vt")) {
            if(gi.getGridlet().getPriority() == 3) {
                Scheduler.high_q.add(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                //scheduler.sim_schedule(scheduler.getEntityId(scheduler.getEntityName()),GridSimTags.SCHEDULE_NOW, 0);
                scheduler.sim_schedule(GridSim.getEntityId("Alea_3.0_scheduler"), 0.0, AleaSimTags.EVENT_SCHEDULE, gi.getGridlet());
                return;    
            }
            if(gi.getGridlet().getPriority() == 1) {
                Scheduler.low_q.add(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                //scheduler.sim_schedule(scheduler.getEntityId(scheduler.getEntityName()),GridSimTags.SCHEDULE_NOW, 0);
                //scheduler.sim_schedule(GridSim.getEntityId("Alea_3.0_scheduler"), 0.0, AleaSimTags.EVENT_SCHEDULE, gi.getGridlet());
                return;    
            }
            if (gi.getQueue().equals("normal_q")) {
                Scheduler.regular_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            
            if (gi.getQueue().equals("vis_q")) {
                Scheduler.regular_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("largemem_q")) {
                Scheduler.regular_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("p100_normal_q")) {
                Scheduler.regular_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("open_q")) {
                Scheduler.regular_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }

            // All remaining non standard queues are considered to be normal
            Scheduler.regular_q.addLast(gi);
        }
        else{
            if(gi.getGridlet().getPriority() == 1) {
                Scheduler.low_q.add(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            } else if(gi.getGridlet().getPriority() == 2) {
                Scheduler.regular_q.add(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            } else if(gi.getGridlet().getPriority() == 3) {
                Scheduler.high_q.add(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                //scheduler.sim_schedule(scheduler.getEntityId(scheduler.getEntityName()),GridSimTags.SCHEDULE_NOW, 0);
                scheduler.sim_schedule(GridSim.getEntityId("Alea_3.0_scheduler"), 0.0, AleaSimTags.EVENT_SCHEDULE, gi.getGridlet());
                return;    
            }
        }
        
        
        /*else if(gi.getGridlet().getInst().equals("vt")) {
            if (gi.getQueue().equals("normal_q")) {
                Scheduler.low_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("dev_q")) {
                Scheduler.high_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("vis_q")) {
                Scheduler.regular_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("largemem_q")) {
                Scheduler.regular_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("p100_dev_q")) {
                Scheduler.high_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("p100_normal_q")) {
                Scheduler.regular_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("open_q")) {
                Scheduler.regular_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }

            // All remaining non standard queues are considered to be normal
            Scheduler.high_q.addLast(gi);
        }
        else if(gi.getGridlet().getInst().equals("uva")) {
            if (gi.getQueue().equals("dev")) {
                Scheduler.high_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("standard")) {
                Scheduler.low_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("largemem")) {
                Scheduler.regular_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("gpu")) {
                Scheduler.regular_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("parallel")) {
                Scheduler.regular_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("knl")) {
                Scheduler.high_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }

            // All remaining non standard queues are considered to be normal
            Scheduler.high_q.addLast(gi);
        }        
        else if(gi.getGridlet().getInst().equals("iu")) {
            if (gi.getQueue().equals("normal")) {
                Scheduler.low_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("debug_gpu")) {
                Scheduler.high_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("debug_cpu")) {
                Scheduler.high_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("cpu16")) {
                Scheduler.high_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("gpu")) {
                Scheduler.regular_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("long")) {
                Scheduler.low_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            if (gi.getQueue().equals("serial")) {
                Scheduler.low_q.addLast(gi);
                Scheduler.runtime += (new Date().getTime() - runtime1);
                return;
            }
            // All remaining non standard queues are considered to be normal
            Scheduler.high_q.addLast(gi);
        }*/
        
        
        
        Scheduler.runtime += (new Date().getTime() - runtime1);

    }

    /**
     * PBS-pro like algorithm. It is setted up according to the situation used
     * in Czech Grid infrastructure "MetaCentrum".<p> For sharcnet, the setup
     * would be different. Therefore, we do not suggest that you use it as it is
     * but rather take it as a inspiration how multi-queue system with
     * priorities may be implemented.
     */
    @Override
    public int selectJob() {
        //System.out.println("Selecting job by PBS PRO...");
        ResourceInfo r_cand = null;
        int scheduled = 0;

        // we go through all queues according to their priority
        for (int q = 0; q < Scheduler.all_queues.size(); q++) {
                        

            // print priorities
            /*
             * for (int i = 0; i < curr_queue.size(); i++) { GridletInfo gi =
             * (GridletInfo) curr_queue.get(i);
             * System.out.print(Math.round(gi.getPriority())+", ");
             * if(i==curr_queue.size()-1)System.out.println();
            }
             */

            Scheduler.queue = Scheduler.all_queues.get(q);
            Collections.sort(Scheduler.queue, new WallclockComparator());
            /*if(!Scheduler.data_set.contains("ccc"))
                
            else{
                if(Scheduler.queue == Scheduler.low_q){
                    Collections.sort(Scheduler.queue, new DeadlineComparator());
                }
            }*/
            for (int i = 0; i < Scheduler.queue.size(); i++) {
                
                GridletInfo gi = (GridletInfo) Scheduler.queue.get(i);
                
                ArrayList<ResourceInfo> rsInfoList = Scheduler.resourceInfoList;
                Collections.sort(rsInfoList, new CostComparator(gi.getGridlet().getInst()));
                
                for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
                    ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
                    if (Scheduler.isSuitable(ri, gi) && ri.canExecuteNow(gi)) {
                        r_cand = ri;
                        break;
                    }
                }
                if (r_cand != null) {
                    gi = (GridletInfo) Scheduler.queue.remove(i);
                    /*
                     * if(i>0 && gi.getPriority() > ((GridletInfo)
                     * curr_queue.get(0)).getPriority()){
                     * System.out.println(i+"th job "+gi.getID()+" selected with
                     * bigger fairshare than 0 job, 0 = "+((GridletInfo)
                     * curr_queue.get(0)).getPriority()+"
                     * curr="+gi.getPriority());
                    }
                     */
                    
                    // set the resource ID for this gridletInfo (this is the final scheduling decision)
                    gi.setResourceID(r_cand.resource.getResourceID());
                    scheduler.submitJob(gi.getGridlet(), r_cand.resource.getResourceID());
                    r_cand.is_ready = true;
                    //scheduler.sim_schedule(GridSim.getEntityId("Alea_3.0_scheduler"), 0.0, AleaSimTags.GRIDLET_SENT, gi);
                    scheduled++;
                    // we removed a job from position i so the next job is now on i
                    // we have to decrease the counter otherwise we would skip a job due to i++ in for loop
                    i--;
                    
                    r_cand.addGInfoInExec(gi);
                    
                    if(gi.getGridlet().getGridletID() == 119486){
                        //System.out.println("paisi");
                        for(int xx = 0; xx < r_cand.resInExec.size(); xx++){
                            GridletInfo gxx = r_cand.resInExec.get(xx);
                            System.out.println(gxx.getID()+ " " + gxx.getGridlet().getGridletStatusString() + " " + gxx.getGridlet().getNumPE());
                        }
                    }
                    r_cand = null;
                    
                    return scheduled;

                }
                else if(gi.getGridlet().getPriority() == 3){
                    if(gi.getGridlet().getPriority() == 3 && (clock() - gi.getGridlet().getArrival_time()) > 600){
                        //gi = (GridletInfo) Scheduler.queue.remove(i);
                        //ExperimentSetup.policy.addNewJob(gi, 2);
                        //return scheduled;
                        continue;
                    }
                    for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
                        ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
                        if (Scheduler.isSuitable(ri, gi) && ri.canExecuteHighPriority(gi)) {
                            r_cand = ri;
                            break;
                        }
                    }
                    if(r_cand != null){
                        //System.err.println(gi.getID() + " needs to be scheduled immediately" );
                        if(r_cand.removeLowPriority(gi, scheduler)){ 
                            /*gi = (GridletInfo) Scheduler.queue.remove(i);
                            r_cand.addGInfoInExec(gi);
                            // set the resource ID for this gridletInfo (this is the final scheduling decision)
                            gi.setResourceID(r_cand.resource.getResourceID());
                            scheduler.submitJob(gi.getGridlet(), r_cand.resource.getResourceID());
                            r_cand.is_ready = true;
                            //scheduler.sim_schedule(GridSim.getEntityId("Alea_3.0_scheduler"), 0.0, AleaSimTags.GRIDLET_SENT, gi);
                            scheduled++;
                            // we removed a job from position i so the next job is now on i
                            // we have to decrease the counter otherwise we would skip a job due to i++ in for loop
                            i--;
                            r_cand = null;
                            //System.out.println(gi.getID()+" send job by PBS PRO..."+Math.round(GridSim.clock()));
                            */
                            r_cand.forceUpdate(clock());
                            return scheduled;
                        }
                        else{
                            //System.err.println("Hoilo na ken ????????????????????????????????");
                            return scheduled;
                        }
                    }else{
                        continue;
                    }
                }

            }//we went through the whole queue

        }// we went through all queues

        return scheduled;

    }
}
