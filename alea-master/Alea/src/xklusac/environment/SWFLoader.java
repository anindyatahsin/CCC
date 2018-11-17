/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.environment;

/**
 *
 * @author Dalibor
 */
import alea.core.AleaSimTags;
import eduni.simjava.Sim_event;
import gridsim.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import xklusac.extensions.*;
import eduni.simjava.distributions.Sim_normal_obj;

/**
 * Class SWFLoader<p>
 * Loads jobs dynamically over time from the file. Then sends these gridlets to
 * the scheduler. SWF stands for Standard Workloads Format (SWF).
 *
 * @author Dalibor Klusacek
 */
public class SWFLoader extends GridSim {

    /**
     * input
     */
    Input r = new Input();
    /**
     * current folder
     */
    String folder_prefix = "";
    /**
     * buffered reader
     */
    BufferedReader br = null;
    /**
     * total number of jobs in experiment
     */
    int total_jobs = 0;
    /**
     * start time (for UNIX epoch converting)
     */
    int start_time = -1;
    /**
     * number of PEs in the "biggest" resource
     */
    int maxPE = 1;
    /**
     * minimal PE rating of the slowest resource
     */
    int minPErating = 1;
    int maxPErating = 1;
    /**
     * gridlet counter
     */
    int current_gl = 0;
    /**
     * data set name
     */
    String data_set = "";
    /**
     * counter of failed jobs (as stored in the GWF file)
     */
    int fail = 0;
    int help_j = 0;
    Random rander = new Random(4567);
    double last_delay = 0.0;
    Sim_normal_obj norm;
    double prevl = -1.0;
    double preve = -1.0;
    int prevc = -1;
    long prevram = -1;
    long prev_job_limit = -1;
    int count = 1;
    int uva_factor = ExperimentSetup.uva_factor ;

    /**
     * Creates a new instance of JobLoader
     */
    public SWFLoader(String name, double baudRate, int total_jobs, String data_set, int maxPE, int minPErating, int maxPErating) throws Exception {
        super(name, baudRate);
        System.out.println(name + ": openning all jobs from " + data_set);
        if (ExperimentSetup.meta) {
            folder_prefix = System.getProperty("user.dir");
        } else {
            folder_prefix = System.getProperty("user.dir");
        }
        if (ExperimentSetup.data) {
            String[] path = folder_prefix.split("/");
            if (path.length == 1) {
                path = folder_prefix.split("\\\\");
            }
            folder_prefix = "";
            for (int i = 0; i < path.length - 1; i++) {
                folder_prefix += path[i] + "/";
            }
            //System.out.println("Adresar = "+adresar);
        }
        br = r.openFile(new File(folder_prefix + "/data-set/" + data_set));
        this.total_jobs = total_jobs;
        this.maxPE = maxPE;
        this.minPErating = minPErating;
        this.maxPErating = maxPErating;
        this.data_set = data_set;
        this.norm = new Sim_normal_obj("normal distr", 0.0, 5.0, (121 + ExperimentSetup.rnd_seed));

    }

    /**
     * Reads jobs from data_set file and sends them to the Scheduler entity
     * dynamically over time.
     */
    public void body() {
        super.gridSimHold(10.0);    // hold by 10 second

        while (current_gl < total_jobs) {

            Sim_event ev = new Sim_event();
            sim_get_next(ev);

            if (ev.get_tag() == AleaSimTags.EVENT_WAKE) {

                ComplexGridlet gl = readGridlet(current_gl);
                current_gl++;
                if (gl == null && current_gl < total_jobs) {
                    super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.EVENT_WAKE);
                    continue;
                } else if (gl == null && current_gl >= total_jobs) {
                    continue;
                }
                // to synchronize job arrival wrt. the data set.
                double delay = Math.max(0.0, (gl.getArrival_time() - super.clock()));
                // some time is needed to transfer this job to the scheduler, i.e., delay should be delay = delay - transfer_time. Fix this in the future.
                //System.out.println("Sending: "+gl.getGridletID());
                last_delay = delay;
                super.sim_schedule(this.getEntityId("Alea_3.0_scheduler"), delay, AleaSimTags.GRIDLET_INFO, gl);

                delay = Math.max(0.0, (gl.getArrival_time() - super.clock()));
                if (current_gl < total_jobs) {
                    // use delay - next job will be loaded after the simulation time is equal to the previous job arrival.
                    super.sim_schedule(this.getEntityId(this.getEntityName()), delay, AleaSimTags.EVENT_WAKE);
                }

                continue;
            }
        }
        System.out.println("Shuting down - last gridlet = " + current_gl + " of " + total_jobs);
        super.sim_schedule(this.getEntityId("Alea_3.0_scheduler"), Math.round(last_delay + 2), AleaSimTags.SUBMISSION_DONE, new Integer(current_gl));
        Sim_event ev = new Sim_event();
        sim_get_next(ev);

        if (ev.get_tag() == GridSimTags.END_OF_SIMULATION) {
            System.out.println("Shuting down the " + data_set + "_PWALoader... with: " + fail + " failed or skipped jobs");
        }
        shutdownUserEntity();
        super.terminateIOEntities();

    }

    /**
     * Reads one job from file.
     */
    private ComplexGridlet readGridlet(int j) {
        String[] values = null;
        String line = "";

        //System.out.println("Read job "+j);
        if (j == 0) {
            while (true) {
                try {
                    for (int s = 0; s < ExperimentSetup.skipJob; s++) {
                        line = br.readLine();
                    }
                    if(!data_set.contains("ccc"))
                        values = line.split("\t");
                    else 
                        values = line.split(",");
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
                if (!values[0].contains(";")) {
                    /*if (line.charAt(0) == ' ') {
                        line = line.substring(1);
                    }
                    if (line.charAt(0) == ' ') {
                        line = line.substring(1);
                    }
                    if (line.charAt(0) == ' ') {
                        line = line.substring(1);
                    }
                    if (line.charAt(0) == ' ') {
                        line = line.substring(1);
                    }*/
                    if(!data_set.contains("ccc"))
                        values = line.split("\\s+");
                    else
                        values = line.split(",");
                    break;
                } else {
                    //System.out.println("error --- "+values[0]);
                }
            }
        } else {
            try {
                line = br.readLine();
                //System.out.println(">"+line+"<");
                /*if (line.charAt(0) == ' ') {
                    line = line.substring(1);
                }
                if (line.charAt(0) == ' ') {
                    line = line.substring(1);
                }
                if (line.charAt(0) == ' ') {
                    line = line.substring(1);
                }
                if (line.charAt(0) == ' ') {
                    line = line.substring(1);
                }*/
                //System.out.println("error1 = "+line+" at gi = "+j);
                if(line == null) return null;
                if(!data_set.contains("ccc"))
                    values = line.split("\\s+");
                else
                    values = line.split(",");

            } catch (IOException ex) {
                System.out.println("error = " + values[0] + " at gi = " + j);
                ex.printStackTrace();
            }
        }
        // such line is not a job description - it is a typo in the SWF file
        if (values.length < 5 || values[1].equals("-1")) {
            fail++;
            System.out.println(j + " returning: null " + values[0]);
            return null;
        }

        // such job failed or was cancelled and no info about runtime or numCPU is available therefore we skip it
        if (values[3].equals("-1") || values[4].equals("-1")) {
            fail++;
            //System.out.println("returning: null2 ");
            return null;
        }
        //System.out.println(values[0]+"+"+values[1]+"+"+values[2] + ": Number parsing error: " + values[4]);
        int id = Integer.parseInt(values[0]);
        int numCPU;
        try {
            numCPU = Integer.parseInt(values[4]);
        } catch (NumberFormatException ex) {
            System.out.println(values[0] + ": Number parsing error: " + values[4]);
            //ex.printStackTrace();
            numCPU = 1;
        }

        // we do not allow more PEs for one job than there is on the "biggest" machine.
        // Co-allocation is only supported over one cluster (GridResource) by now.
        if (numCPU > maxPE) {
            numCPU = maxPE;

        }

        long arrival = 0;
        // synchronize GridSim's arrivals with the UNIX epoch format as given in GWF
        if (start_time < 0) {
            //System.out.println("prvni: "+j+" start at:"+values[1]+" line="+line);
            start_time = Integer.parseInt(values[1]);
            arrival = 0;

        } else {

            arrival = ((Integer.parseInt(values[1]) - start_time));
            //System.out.println("pokracujeme..."+arrival);

        }
        arrival = Math.round(new Double(arrival) / ExperimentSetup.arrival_rate_multiplier);
        String institute = values[18];
        // minPErating is the default speed of the slowest machine in the data set        
        double length = Math.round((Integer.parseInt(values[3])) * maxPErating);
        if(length <= 10){
            length = 10;
        }
        if(institute.equals("uva")){
            length = length * uva_factor;
        }
        // queue name
        String queue = values[14];

        // requested RAM = KB per node (not CPU)
        long ram = -1;
        try{
            ram = Long.parseLong(values[9]);
        }catch(NumberFormatException e){
            ram = (long)(Double.parseDouble(values[9]) * 1000);
        }
        
        if (ram == -1) {
            if (Long.parseLong(values[9]) == -1) {
                //System.out.println(id + " not specified RAM, setting 1 KB...");
                ram = 1;
            } else {
                ram = Long.parseLong(values[9]);
            }
        } else {
            if (data_set.contains("zewura") || data_set.contains("wagap") || data_set.contains("meta") || data_set.contains("ncbr")) {

                ram = Math.round(ram / 1024.0);
                queue = values[19];
            }
            double gbram = Math.round(ram * 10 / 1048576.0) / 10.0;
            //System.out.println(id+ " requests "+ram+" KB RAM, "+gbram+" GB RAM per "+numCPU+" CPUs");
        }
        if (!ExperimentSetup.use_RAM) {
            ram = 0;
        }

        // skip such job
        /*if (data_set.contains("zewura") || data_set.contains("wagapp") || data_set.contains("meta")) {
         if (!queue.equals("short") && !queue.equals("long") && !queue.equals("normal") && !queue.equals("backfill") && !queue.equals("preemptible")) {
         fail++;
         return null;
         }
         }
         */
        //SKIP
        /*
         * if (id < 172262) { fail++; return null; }
         */
        

        // finally create gridlet
        //numCPU = 1;
        long job_limit = Integer.parseInt(values[8]);
        if(job_limit <= 10){
            job_limit = 10;
        }
        if(institute.equals("uva")){
            job_limit = job_limit * uva_factor;
        }
        if (job_limit < 0) {
            // atlas = 432000
            // thunder = 432000
            if (data_set.equals("thunder.swf")) {
                job_limit = 48000; //13 hours 20 min
                ExperimentSetup.max_estim++;
            } else if (data_set.equals("atlas.swf")) {
                job_limit = 73200; //20 hours 20 minutes
                ExperimentSetup.max_estim++;
            } else if (data_set.equals("star.swf")) {
                job_limit = 64800; //18 hours
                ExperimentSetup.max_estim++;
            } else if (data_set.equals("ctc-sp2.swf")) {
                job_limit = 64800; //18 hours
                ExperimentSetup.max_estim++;
            } else if (data_set.equals("blue.swf")) {
                job_limit = 7200; //2 hours
                ExperimentSetup.max_estim++;
            } else if (data_set.equals("kth-sp2.swf")) {
                job_limit = 14400; //4 hours
                ExperimentSetup.max_estim++;
            } else if (data_set.equals("sandia.swf")) {
                job_limit = 18000; //5 hours
                ExperimentSetup.max_estim++;
            } else {
                job_limit = Integer.parseInt(values[3]);
            }
        }

        double estimatedLength = 0.0;
        if (ExperimentSetup.estimates) {
            //roughest estimate that can be done = queue limit        
            estimatedLength = Math.round(Math.max((job_limit * maxPErating), length));
            //System.out.println(id+" Estimates "+estimatedLength+" real = "+length);
        } else {
            // exact estimates
            estimatedLength = length;
            //System.out.println(id+" Exact "+estimatedLength);
        }

        String user = values[11];
        int priority = 0;
        
        double deadline = Double.MAX_VALUE;
        
        if(values[13].equals("lp")){
            priority = 1;
        } else if(values[13].equals("mp")){
            priority = 2;
        } else if(values[13].equals("hp")){
            priority = 3;
            deadline = 10 * 60;
        
        }

        //System.out.println(id + " requests " + ram + " KB RAM per " + numCPU + " CPUs, user: " + user + ", length: " + length + " estimatedLength: " + estimatedLength);
        int numNodes = -1;
        int ppn = -1;
        String properties = "";
        try{
            numNodes = Integer.parseInt(values[16]);
        }catch(NumberFormatException e){
            String[] s = values[16].split("\\+");
            numNodes = s.length;
            System.out.println("Job " + id + " requested node " + s[0] + " and length " + numNodes);
        }
        try{
            ppn = Integer.parseInt(values[17]);
        }catch(NumberFormatException e){
            System.out.println(id);
            e.printStackTrace();
        } 
        
        if(values.length > 19){
            if(values[19].equals("-1")){
                properties = "";
            }
            else{
                properties = values[19].split("=")[0];
                if(properties.contains("gpus")){
                    properties = "gpu";
                }
            }
            if(data_set.contains("ccc")){
                if(institute.equals("vt")){        
                    if(queue.contains("largemem")){
                        if(properties.equals(""))
                            properties += "largemem";
                        else{
                            properties += ",largemem";
                        }   
                    }
                    else if(queue.contains("vis") || queue.contains("p100")){
                        if(properties.equals(""))
                            properties += "gpu";
                        else{
                            properties += ",gpu";
                        }
                    }
                }
                else if(institute.equals("uva")){
                    if(queue.contains("largemem") || queue.contains("knl") || queue.contains("gpu")){
                        if(properties.equals(""))
                            properties += queue;
                        else{
                            properties += "," + queue;
                        }
                    }
                }
                else if(institute.equals("iu")){
                    if(queue.contains("gpu")){
                        if(properties.equals(""))
                            properties += "gpu";
                        else{
                            properties += ",gpu";
                        }
                        
                    }
                    else if(queue.contains("shared") || queue.contains("batch")){
                        if(properties.equals(""))
                            properties += "largemem";
                        else{
                            properties += ",largemem";
                        }
                    }
                }
                
            }
            else if(data_set.contains("vt")){        
                if(queue.contains("normal_q") || queue.contains("largemem") || queue.contains("vis") || queue.contains("open") || queue.contains("p100")){
                    if(properties.equals(""))
                        properties += queue;
                    else{
                        properties += "," + queue;
                    }   
                }
            }
            else if(data_set.contains("uva")){
                if(queue.contains("largemem") || queue.contains("knl") || queue.contains("gpu")){
                    if(properties.equals(""))
                        properties += queue;
                    else{
                        properties += "," + queue;
                    }
                }
            }
            else if(data_set.contains("iu")){
                if(queue.contains("gpu")){
                    if(properties.equals(""))
                        properties += "gpu";
                    else{
                        properties += ",gpu";
                    }
                    if(data_set.contains("iu")){
                        properties += ",bigred";
                    }
                }
                else if(queue.contains("shared") || queue.contains("batch")){
                    if(properties.equals(""))
                        properties += "largemem";
                    else{
                        properties += ",largemem";
                    }
                    if(data_set.contains("iu")){
                        properties += ",mason";
                    }
                }
                else{
                    if(data_set.contains("iu")){
                        if(properties.equals(""))
                            properties += "bigred";
                        else{
                            properties += ",bigred";
                        }
                    }
                }
                
            }
        }
        
        //System.out.println(institute);
        /*if(!data_set.contains("ccc")){
            if (values.length > 19) {
                if(!values[19].equals("-1")){
                    properties = values[19].split("=")[0];
                    if(properties.contains("gpus")){
                        properties = "gpu";
                    }
                        
                    if(data_set.contains("vtech")){
                        
                        if(queue.contains("normal_q") || queue.contains("largemem") || queue.contains("vis") || queue.contains("open") || queue.contains("p100")){
                            properties += "," + queue;
                            deadline = Math.max(job_limit * 2, length + 30 * 60);
                        }
                        else{
                            deadline = Math.min(job_limit * 2, length + 30 * 60);
                        }
                    }
                    if(data_set.contains("uva")){
                        if(queue.contains("largemem")){
                            properties += "," + queue;
                            deadline = Math.max(job_limit * 2, length + 30 * 60);
                        }
                        else if(queue.contains("knl") || queue.contains("gpu")){
                            properties += "," + queue;
                            deadline = Math.min(job_limit * 2, length + 30 * 60);
                        }
                        else if(!(queue.contains("standard") || queue.contains("parallel"))){
                            deadline = Math.min(job_limit * 2, length + 30 * 60);
                        }
                    }
                    else if(data_set.contains("iu")){
                        if(queue.contains("debug") || queue.contains("cpu")){
                            deadline = Math.min(job_limit * 2, length + 30 * 60);
                        }
                        if(queue.contains("gpu")){
                            deadline = Math.max(job_limit * 2, length + 30 * 60);
                            properties += ",gpu";
                        }
                    }
                }
                else if(data_set.contains("vtech")){ 
                    
                    if(queue.contains("normal_q") || queue.contains("largemem") || queue.contains("vis") || queue.contains("open") || queue.contains("p100")){
                        properties += queue;
                        deadline = Math.max(job_limit * 2, length + 30 * 60);
                    }
                    else{
                        deadline = Math.min(job_limit * 2, length + 30 * 60);
                    } 
                    if(queue.contains("dev_q")){
                        deadline = Math.min(job_limit * 2, length + 30 * 60);
                    }
                }
                else if(data_set.contains("uva")){
                    if(queue.contains("largemem")){
                        properties += queue;
                        deadline = Math.max(job_limit * 2, length + 30 * 60);
                    }
                    else if(queue.contains("knl") || queue.contains("gpu")){
                        properties += queue;
                        deadline = Math.min(job_limit * 2, length + 30 * 60);
                    }
                    else if(!(queue.contains("standard") || queue.contains("parallel"))){
                        deadline = Math.min(job_limit * 2, length + 30 * 60);
                    }
                }
                else if(data_set.contains("iu")){

                    if(queue.contains("debug") || queue.contains("cpu")){
                        deadline = Math.min(job_limit * 2, length + 30 * 60);
                    }
                    if(queue.contains("gpu")){
                        properties += "gpu";
                        deadline = Math.max(job_limit * 2, length + 30 * 60);
                    }
                }


                if (data_set.contains("zewura")) {
                    numNodes = 1;
                    ppn = numCPU;
                } else if (data_set.contains("hpc2n")) {
                    ppn = 2;
                    if (numCPU < ppn) {
                        ppn = numCPU;
                        numNodes = 1;
                    } else if (numCPU % 2 == 1) {
                        ppn = 1;
                        numNodes = numCPU;
                    } else {
                        Long nn = Math.round(Math.ceil(numCPU / ppn));
                        numNodes = nn.intValue();
                    }
                    if (ppn * numNodes != numCPU) {
                        System.out.println(id + ": numNodes value is wrong, CPUs = " + numCPU + " ppn = " + ppn);
                    }
                }

                if (numCPU / numNodes != ppn) {
                    System.out.println(id + ": CPUs mismatch CPUs = " + numCPU + " ppn = " + ppn + " nodes = " + numNodes);
                    numCPU = ppn * numNodes;
                }
            }
        } else{
            if (values.length > 19) {
                if(!values[19].equals("-1")){
                    properties = values[19].split("=")[0];
                    if(properties.contains("gpus")){
                        properties = "gpu,";
                    }else{
                        properties += ",";
                    }
                }else{
                    properties = "";
                }
                if(institute.equals("vt")){
                    if(queue.equals("normal_q") || queue.contains("open")){
                        // properties += "," + queue;
                    }
                    else if(queue.contains("largemem")){
                        properties += "largemem";
                    }
                    else if(queue.contains("vis")){
                        properties += "gpu";
                    }    
                    else if(queue.contains("p100")){
                        properties += "gpu";
                        //deadline = Math.min(job_limit * 2, length + 30 * 60);
                    }
                    else{
                        deadline = Math.min(job_limit * 2, length + 30 * 60);
                    }
                }
                else if(institute.equals("uva")){
                    if(queue.contains("largemem")){
                        properties += queue;
                    }
                    else if(queue.contains("knl") ){
                        properties += queue;
                        deadline = Math.min(job_limit * 2, length + 30 * 60);
                    }
                    else if(queue.contains("gpu")){
                        properties += queue;
                        deadline = Math.max(job_limit * 2, length + 30 * 60);
                    }
                    else if(!(queue.contains("standard") || queue.contains("parallel"))){
                        deadline = Math.min(job_limit * 2, length + 30 * 60);
                    } else{
                        //deadline = Math.min(job_limit * 2, length + 30 * 60);
                    }
                }
                else if(institute.equals("iu")){
                    if(queue.contains("debug") || queue.contains("cpu")){
                        deadline = Math.min(job_limit * 2, length + 30 * 60);
                    }
                    if(queue.contains("gpu")){
                        properties += "gpu";
                    }
                }



                if (data_set.contains("zewura")) {
                    numNodes = 1;
                    ppn = numCPU;
                } else if (data_set.contains("hpc2n")) {
                    ppn = 2;
                    if (numCPU < ppn) {
                        ppn = numCPU;
                        numNodes = 1;
                    } else if (numCPU % 2 == 1) {
                        ppn = 1;
                        numNodes = numCPU;
                    } else {
                        Long nn = Math.round(Math.ceil(numCPU / ppn));
                        numNodes = nn.intValue();
                    }
                    if (ppn * numNodes != numCPU) {
                        System.out.println(id + ": numNodes value is wrong, CPUs = " + numCPU + " ppn = " + ppn);
                    }
                }

                if (numCPU / numNodes != ppn) {
                    System.out.println(id + ": CPUs mismatch CPUs = " + numCPU + " ppn = " + ppn + " nodes = " + numNodes);
                    numCPU = ppn * numNodes;
                }
                
            }
            
        }*/
        
        
        if(numNodes > 1){
        //System.out.println(id + " " + institute + " " + properties + " " + numNodes + " " + numCPU);
            /*if(!properties.equals(""))
                properties += "mpi";
            else
                properties += "mpi";
            */
            if(properties.endsWith(",") || properties.equals("")){
                properties += "mpi";
            }else{
                properties += ",mpi";
            }
        }
        //if(length < 1*60 && data_set.contains("ccc")){
        //System.out.println(id + " " + institute + " " + properties + " " + numNodes + " " + numCPU);
            /*if(!properties.equals(""))
                properties += "mpi";
            else
                properties += "mpi";
            */
            /*if(properties.endsWith(",") || properties.equals("")){
                properties += institute;
            }else{
                properties += "," + institute;
            }*/
        //}
        
        // obsolete and useless
        double perc = norm.sample();

        job_limit = Math.max(1, Math.round(job_limit / ExperimentSetup.runtime_minimizer));
        length = Math.max(1, Math.round(length / ExperimentSetup.runtime_minimizer));
        estimatedLength = Math.max(1, Math.round(estimatedLength / ExperimentSetup.runtime_minimizer));

        if (data_set.contains("wagap")) {
            if (ppn > 8) {
                properties += ":^cl_zigur";
            }
            if (ppn > 12) {
                properties += ":^cl_zegox";
            }
            if (ppn > 16) {
                properties += ":^cl_zapat";
            }
        }
        
        if (queue.equals("backfill") && data_set.contains("meta")) {
            properties += ":^cl_manwe:^cl_mandos:^cl_skirit:^cl_ramdal:^cl_haldir:^cl_gram";
        }

        if (queue.equals("mikroskop") || queue.equals("quark")) {
            properties += ":cl_quark";
        }

        if (queue.contains("ncbr")) {
            properties += ":cl_perian";
        }
        
        if(!Scheduler.all_queues_names.contains(queue) && ExperimentSetup.use_queues){
            fail++;
            System.out.println("Unknown queue "+queue+" - skipping job "+id);
            return null;
        }

        // manually established - fix it according to your needs
        
        //System.out.println("creating job:" + id + " " + user + " " + job_limit + " " + new Double(length) + " " + estimatedLength + " " + 10 + " " + 10 + " " +
        //        "Linux" + " " + "Risc arch." + " " + arrival + " " + deadline + " " + 1 + " " + numCPU + " " + 0.0 + " " + queue + " " + properties + " " + perc + " " + ram + " " + numNodes + " " + ppn);
        
        int array_size = Integer.parseInt(values[15]);
        
        
        
        ComplexGridlet gl = new ComplexGridlet(id, user, job_limit, new Double(length), estimatedLength, 10, 10,
                "Linux", "Risc arch.", arrival, deadline, priority, numCPU, 0.0, queue, properties, perc, ram, numNodes, ppn, institute,
                array_size);

        // and set user id to the Scheduler entity - otherwise it would be returned to the JobLoader when completed.
        //System.out.println(id+" job has limit = "+(job_limit/3600.0)+" queue = "+queue);
        gl.setUserID(super.getEntityId("Alea_3.0_scheduler"));
        return gl;
    }
}
