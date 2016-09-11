/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;
//import javax.swing.JOptionPane;

/**
 *
 * @author harsh
 */
class MonitorTaskTreacker implements Runnable
{
    public int sec;
    public boolean run=true;
    IJobTracker tempStub;
    public static byte[] arr2,arr1;
    
    public MonitorTaskTreacker(int sec,IJobTracker tempStub) {
    this.sec=sec;
    this.tempStub=tempStub;
    }
    
    public  synchronized void shutdown(){
        this.run=false;
    }

    @Override
    @SuppressWarnings("UseSpecificCatch")
    public  synchronized    void   run() {
       // throw new UnsupportedOperationException("Not supported yet.");
        TaskTracker obj=new TaskTracker();
        int count =0;
        while(run)
        {
            try {
               // System.out.println(TaskTracker.executor1.getCorePoolSize()+" activated"+TaskTracker.executor1.getActiveCount());
                //System.out.println("this is count "+count++);
                arr1=TaskTracker.encodeHeartBeatRequest(TaskTracker.taskTrackerId,TaskTracker.numMapSlotsFree,TaskTracker.numReduceSlotsFree,TaskTracker.mapQueue,TaskTracker.reduceQueue);
                //arr1="aman".getBytes();
                //System.out.println("heart is beating in TaskTrack");
                arr2=tempStub.heartBeat(arr1);
                obj.decodeHeartBeatResponse(arr2);
                Thread.sleep(1000*sec);
            } catch (Exception ex) {
                Logger.getLogger(MonitorTaskTreacker.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    
}

class Job implements IMapper, Runnable
    {

        int blockNumber;
        int port;
        String ip;
        String fileName;
        int taskId;
        int jobId;
        public Job(int blockNumber,String ip,int port,int taskId,int jobId)
        {
            this.blockNumber=blockNumber;
            this.fileName=fileName;
            this.ip=ip;
            this.port=port;
            this.taskId=taskId;
            this.jobId=jobId;
        }
        @Override
        public  synchronized   String map(String a) {
            PrintWriter writer1=null;
                try {
                  //  System.out.println(TaskTracker.executor1.getCorePoolSize()+" activated"+TaskTracker.executor1.getActiveCount());
                  //  System.out.println("this is ip and port"+ip+":"+port);
                    Registry registry = LocateRegistry.getRegistry(ip,port);
                    IDataNode tempStub = (IDataNode) registry.lookup("DN"+ip);
                    
                    byte[] data=tempStub.readBlock(blockNumber);
                    String s=new String(data);
                    String[] s1=s.split("\n");
               //     System.out.println("map is running");
                    int count=0;
                    String word=TaskTracker.read_word_file_for_jt_ip_port();
                    String f="job_"+jobId+"_map_"+taskId;
                   writer1 = new PrintWriter(f);
                 //   System.out.println("writing file job_taskid in mapper");
                        String line;
                        for(int i=0;i<s1.length;i++)
                        {
                            if(s1[i].contains(word))
                            {
                                writer1.println(s1[i]);
                                count++;
                            }
                        }
                    writer1.close();
                    MyClient.put(f);
                  //  System.out.println("file uploaded to hdfs in mappper");
                    TaskTracker.poolQueue.put(jobId+""+taskId, Boolean.TRUE);
                    TaskTracker.numMapSlotsFree+=1;
                  //  */
		} 
                catch (Exception ex)
                {
                        Logger.getLogger(TaskTracker.class.getName()).log(Level.SEVERE, null, ex);
		}
                return "done";
        }
      
        @Override
        public  synchronized   void run() {
            try{
            map("call");
            }
            catch(Exception e){System.out.println("error in map call run");}
        }
    
    }
    
    class jobReduce implements IReducer,Runnable
        {
        
        int jobId;
        int taskId;
        String reducerName;
        String outputFile;
        String[] mapOutputFiles;
        public jobReduce(int jobId, int taskId, String reducerName, String[] mapOutputFiles, String outputFile) {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            this.jobId=jobId;
            this.taskId=taskId;
            this.mapOutputFiles=mapOutputFiles;
            this.reducerName=reducerName;
            this.outputFile=outputFile;
        }

            @Override
            public  synchronized   String reduce(String s)
            {
                //MyClient obj=new MyClient();
                String [] s1=new String[mapOutputFiles.length];
                System.out.println("reducer started"+taskId); 
                for(int i=0;i<mapOutputFiles.length;i++)
                {//System.out.println("this is mapoutputfiles in reducer "+i+"th "+mapOutputFiles[i]);
                    try
                    {
                      MyClient.get(mapOutputFiles[i]);
                      System.out.println(taskId+" file got in reducer from reducer " +mapOutputFiles[i]);
                    } 
                    catch (Exception ex)
                    {
                        Logger.getLogger(jobReduce.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                String f1="";
                PrintWriter pw=null;
                f1=s+"_"+jobId+"_"+taskId+"";
            try {
                //f1=s+"_"+jobId+"_"+taskId+"";
                pw=new PrintWriter(f1);
               // System.out.println("file name is "+f1);
                int count=0;
                for(int i=0;i<mapOutputFiles.length;i++)
                {
                        FileReader fileReader=null;
                        String line;
                        String fileName=mapOutputFiles[i];
                        File f = new File(fileName);
                        fileReader = new FileReader(f);
                        BufferedReader bufferedReader = new BufferedReader(fileReader);
                        while((line=bufferedReader.readLine())!=null)
                        {   count++;
                             pw.println(line);
                        }
                        //System.out.println("count is1 "+count);
                        fileReader.close();
                        //System.out.println("count is2 "+count);
                }
                pw.close();
                //System.out.println("all files merged in reducer");
            }
            catch(Exception e)
                {
                    System.out.println("Error In Reduce. in writing....");
                }try{MyClient.put(f1);
                System.out.println("uploader to hdfs in reducer named "+s+taskId);
                TaskTracker.poolQueue_reducer.put(jobId+""+taskId, Boolean.TRUE);
                TaskTracker.numReduceSlotsFree+=1;
                }
                catch(Exception e)
                {
                    System.out.println("Error In Reduce.....");
                }
                
                return "DONE";
            }

            @Override
            public  synchronized   void run() {
                try{
                reduce(outputFile);}
                catch(Exception e){System.out.println("error in reducer call in run");}
            }
            
        }

public class TaskTracker {

    /**
     * @param args the command line arguments
     */
    public static String jt_ip="";
    public static int jt_port=0;
    public static  byte[] arr1;
    public static int taskTrackerId;
            public static int numMapSlotsFree; 
            public static int numReduceSlotsFree;
            public static MapperTask map1;
            public static MapperTask map2;
            public static List<MapperTask> mapQueue;
            public static List<ReducerTask> reduceQueue;
            public static ReducerTask reduce1;
            public static int status;
            public static ReducerTask reduce2;
            public static ThreadPoolExecutor executor1;
            public static ThreadPoolExecutor executor2;
            public static Map<String,Boolean> poolQueue=new HashMap<String, Boolean>();
            public static Map<String,Boolean> poolQueue_reducer=new HashMap<String, Boolean>();
            
        public static void main(String[] args) {
        try {
            //try {
                // TODO code application logic here
                
                read_con_file_for_jt_ip_port();
                //System.out.println(jt_ip+"--"+jt_port);
                
                Registry registry = LocateRegistry.getRegistry(jt_ip,jt_port);	
                IJobTracker tempStub = (IJobTracker) registry.lookup("JT"+jt_ip);
                //System.out.println("CLIENT IS UP");
                
                executor1= (ThreadPoolExecutor)Executors.newFixedThreadPool(5);
                executor2= (ThreadPoolExecutor)Executors.newFixedThreadPool(5);
                
                 numMapSlotsFree=5;
                 numReduceSlotsFree=5;
                
                mapQueue=new ArrayList<MapperTask>();
                reduceQueue=new ArrayList<ReducerTask>();
                MonitorTaskTreacker mtt=new MonitorTaskTreacker(3, tempStub);
                Thread t = new Thread(mtt);
                t.start();
                //mtt.shutdown();
                
        }
        catch (Exception ex) {
            Logger.getLogger(TaskTracker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public  synchronized    void decodeHeartBeatResponse(byte[] arr2) {
        try {
            //throw new UnsupportedOperationException("Not yet implemented");
            MapReduce.HeartBeatResponse res1=MapReduce.HeartBeatResponse.parseFrom(arr2);
            status=res1.getStatus();
            System.out.println(res1.getStatus()+"decode");
            
            for(int i=0;i<res1.getMapTasksCount();i++)
            {
                System.out.println("entered in  mapper");
             MapReduce.MapTaskInfo res2=res1.getMapTasks(i);
             int jobId=res2.getJobId();
             int taskId=res2.getTaskId();
             String mapperName=res2.getMapName();
             /* System.out.println(res2.getJobId());
              System.out.println(res2.getTaskId());
              System.out.println(res2.getMapName());
             */ MapReduce.BlockLocations res3=res2.getInputBlocks();
              int blockNumber=res3.getBlockNumber();
              //System.out.println(res3.getBlockNumber());
              MapReduce.DataNodeLocation res4=res3.getLocations();
              String ip=res4.getIp();
              int port=res4.getPort();
              MapperTask obj=new MapperTask(jobId,taskId,blockNumber , ip, port, mapperName);
              mapQueue.add(obj);
             // System.out.println(res4.getIp());
             // System.out.println(res4.getPort());
              
              poolQueue.put(jobId+""+taskId, Boolean.FALSE);
              Job job=new Job(blockNumber,ip,port,taskId,jobId);
              executor1.execute(job);
              
                TaskTracker.numMapSlotsFree=executor1.getCorePoolSize()-executor1.getActiveCount();
        //        System.out.println(executor1.getActiveCount()+"--->"+TaskTracker.numMapSlotsFree);
            }
            //System.out.println("heading towards reducer request decode");
            for(int i=0;i<res1.getReduceTasksCount();i++)
            {
                //System.out.println("reducer request reached"+res1.getReduceTasksCount());
                MapReduce.ReducerTaskInfo res2=res1.getReduceTasks(i);
                int jobId=res2.getJobId();
                int taskId=res2.getTaskId();
                String reducerName=res2.getReducerName();
                String outputFile=res2.getOutputFile();
                int c=res2.getMapOutputFilesCount();
                String []s=new String [c];
                
                //res2.getMapOutputFilesList();
                for(int j=0;j<res2.getMapOutputFilesCount();j++)
                {
                    System.out.println("this is inside decode to find num of file "+res2.getMapOutputFilesCount());
                    s[j]=res2.getMapOutputFiles(j);
                }
                
                ReducerTask  obj=new ReducerTask(jobId, taskId, reducerName, s, outputFile);
                reduceQueue.add(obj);
               // System.out.println("this is decode output file"+outputFile);
                poolQueue_reducer.put(jobId+""+taskId, Boolean.FALSE);
                jobReduce job=new jobReduce(jobId,taskId,reducerName,s,outputFile);
                executor2.execute(job);
                
                TaskTracker.numReduceSlotsFree=executor2.getCorePoolSize()-executor2.getActiveCount();
               //System.out.println(executor2.getActiveCount()+"--->"+TaskTracker.numReduceSlotsFree);
                
            }
        } catch (InvalidProtocolBufferException ex) {
            Logger.getLogger(TaskTracker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public   synchronized  static byte[] encodeHeartBeatRequest(int taskTrackerId, int numMapSlotsFree, int numReduceSlotsFree, List<MapperTask> mapQueue, List<ReducerTask> reduceQueue) {
        //throw new UnsupportedOperationException("Not yet implemented");
        MapReduce.HeartBeatRequest.Builder req1= MapReduce.HeartBeatRequest.newBuilder();
        req1.setTaskTrackerId(taskTrackerId);
        req1.setNumMapSlotsFree(numMapSlotsFree);
        req1.setNumReduceSlotsFree(numReduceSlotsFree);
        
      //  System.out.println("encoding for heartbeat map queue size "+mapQueue.size());
        int count=mapQueue.size()-1;
        for(int i=mapQueue.size()-1;i>=0;i--)
        {
            
        //   System.out.println("count in ecoding is "+count+" "+i);
           MapReduce.MapTaskStatus.Builder req2=MapReduce.MapTaskStatus.newBuilder();
           req2.setJobId(mapQueue.get(i).jobId);
           req2.setTaskId(mapQueue.get(i).taskId);
           req2.setMapOutputFile("job_"+mapQueue.get(i).jobId+"_map_"+mapQueue.get(i).taskId);
           boolean x=poolQueue.get(mapQueue.get(i).jobId+""+mapQueue.get(i).taskId);
        //    System.out.println(mapQueue.size()+"   size of mapper queue "+i);
           if((!poolQueue.isEmpty())&&x)
           {
               System.out.println("mapper completed and heading towards reducer");
               req2.setTaskCompleted(Boolean.TRUE);
        //       System.out.println(mapQueue.size()+"   size of mapper queue "+i);
                MapperTask obj=mapQueue.get(i);
               poolQueue.remove(obj.jobId+""+obj.taskId);
               mapQueue.remove(obj);
              
           }
           else
           {
                req2.setTaskCompleted(Boolean.FALSE);
           }
        //   System.out.println("this is before "+(count-i));
            req1.addMapStatus(req2);
            
          
        }
       // System.out.println("encoding of mapper completed");
        for(int i=0;i<reduceQueue.size();i++)
        {
            //System.out.println("this is reducer from ecoding");
            MapReduce.ReduceTaskStatus.Builder req3=MapReduce.ReduceTaskStatus.newBuilder();
            req3.setJobId(reduceQueue.get(i).jobId);
            req3.setTaskId(reduceQueue.get(i).taskId);
            
           boolean x=poolQueue_reducer.get(reduceQueue.get(i).jobId+""+reduceQueue.get(i).taskId);
           if((!poolQueue_reducer.isEmpty())&&x)
           {
               
               req3.setTaskCompleted(true);
               ReducerTask obj=reduceQueue.get(i);
              // System.out.println("removing reducer task from queue");
               poolQueue_reducer.remove(obj.jobId+""+obj.taskId);
               reduceQueue.remove(obj);
           }
           else
           {
                req3.setTaskCompleted(false);
           }
            
           req1.addReduceStatus( req3);
        }
        
        return req1.build().toByteArray();
    }

    public synchronized  static   void read_con_file_for_jt_ip_port()
    {
            File f = new File("configuration_file.txt");
            try {
		    FileReader fileReader = new FileReader(f);
                    BufferedReader bufferedReader = new BufferedReader(fileReader);
                    StringBuffer stringBuffer = new StringBuffer();
                    String line;
                    line = bufferedReader.readLine();
                    fileReader.close();
                    //System.out.println("Contents of file:");
                    //System.out.println(line);
                    String [] s=line.split(":");
                    jt_ip=s[0];
                    jt_port=Integer.parseInt(s[1]);
		} 
                catch (Exception e)
                {
                    System.out.println("Exceptio"
                            + "n in config file read");
			e.printStackTrace();
		}
    }
    public  synchronized static   String read_word_file_for_jt_ip_port()
    {
         String line=null;
            File f = new File("word.txt");
            try {
		    FileReader fileReader = new FileReader(f);
                    BufferedReader bufferedReader = new BufferedReader(fileReader);
                    StringBuffer stringBuffer = new StringBuffer();
                   
                    line = bufferedReader.readLine();
                    fileReader.close();
                    
		} 
                catch (Exception e)
                {
                    System.out.println("Exception in config file read");
			e.printStackTrace();
		}
            return line;
    }
}

class MapperTask
{
    int jobId;
    int taskId;
    String mapperName;
    int blockNumber;
    String ip;
    int port;
    boolean taskCompleted;
    
    public MapperTask(int jobId,int taskId,int blockNumber,String ip,int port,String mapName)
    {
        this.jobId=jobId;
        this.taskId=taskId;
        this.blockNumber=blockNumber;
        this.ip=ip;
        this.port=port;
        this.mapperName=mapName;
    }
    
}

class ReducerTask
{
    int jobId;
    int taskId;
    String reducerName;
    String[] mapOutputFiles;
    String outputFile;
    boolean taskCompleted;
    
    public ReducerTask(int jobId, int taskId, String reducerName, String[] mapOutputFile, String outputFile) 
    {
        this.jobId = jobId;
        this.taskId = taskId;
        this.reducerName = reducerName;
        mapOutputFiles=new String[mapOutputFile.length];
        for(int i=0;i<mapOutputFile.length;i++)
            mapOutputFiles[i] = mapOutputFile[i];
        this.outputFile = outputFile;
    }   
 }


/*
Removing from queue;
Set Status
Reducer
*/