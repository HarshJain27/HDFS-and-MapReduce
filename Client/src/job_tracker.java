
import com.google.protobuf.InvalidProtocolBufferException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/*class MapperTask
{
    int jobId;
    int taskId;
    String mapperName;
    int blockNumber;
    int ip;
    int port;
    boolean taskCompleted;
    
    public MapperTask(){}
    
    public MapperTask(int jobId,int taskId,int blockNumber,int ip,int port,String mapName)
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

    public ReducerTask(){}
    
    public ReducerTask(int jobId, int taskId, String reducerName, String[] mapOutputFiles, String outputFile) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.reducerName = reducerName;
        this.mapOutputFiles = mapOutputFiles;
        this.outputFile = outputFile;
    }   
 }
*/
class new_job
{
        public  String mapper_name;//args[0];
        public  String reducer_name;
        public  String file_name;
        public  String output_file;
        public int reducer_count;
        public int num_of_mappers;
        public int[] task_array;
        public String[] map_task_output_file;
        public int diff,diff1;
        public float percent_map=0;
        public float percent_reduce=0;
        public boolean com;
        public new_job(){}
        public  new_job(String m,String r,String file,String out,int count,int total)
        {
            com=false;
            mapper_name=m;
            reducer_name=r;
            file_name=file;
            output_file=out;
            reducer_count=count;
            num_of_mappers=total;
           // System.out.println("total is "+total);
            task_array=new int[num_of_mappers];
            diff=reducer_count;
            diff1=total;
           // System.out.println("num of mapper in new job"+num_of_mappers);
            for(int i=0;i<num_of_mappers;i++)
            {
                task_array[i]=1;
            }
            map_task_output_file=new String[num_of_mappers];
        }
        public synchronized    int update(int task_id,boolean status,String file_name1)
        {
            if(status==true)
            {
                task_array[task_id]=0;
                map_task_output_file[task_id]=file_name1;
                diff1--;
                System.out.println("diff1 is"+diff1);
            }
            percent_map=(num_of_mappers-diff1)/num_of_mappers;
            if(diff1==0)
                return 1;
            return 0;
        }
        public  synchronized   int update_reducer(int task_id,boolean  status)
        {
            if(status==true)
            {
                task_array[task_id]=0;
               // map_task_output_file[task_id]=file_name;
               System.out.println("diff1 is"+diff1);
                diff--;
            }
            percent_reduce=(reducer_count-diff)/reducer_count;
            if(diff==0)
            {com=true;   return 1;}
            return 0;
        }
}

class new_task_mapper
{
    public int job_id;
    public int task_id;
    public String mapper_name;
    public int block_num;
    public DataNodeLocation obj;
    public new_task_mapper(){}
    public new_task_mapper(int j,int t,String m,int block,DataNodeLocation d)
    {
        job_id=j;
        task_id=t;
        mapper_name=m;
        block_num=block;
        obj=d;
    }
}

class new_task_reducer
{
    public int job_id;
    public int task_id;
    public String reducer_name;
    public String[] map_output_file;
    public int diff;
    public String outputfile;
    public new_task_reducer(int j,int t,String r,String[] file,String s)
    {
        job_id=j;
        task_id=t;
        reducer_name=r;
        map_output_file=new String[file.length];
        for(int i=0;i<file.length;i++)
            map_output_file[i]=file[i];
        outputfile=s;
        
    }
}

public class job_tracker implements IJobTracker
{
        public static String static_ip="10.0.0.";
        int  job_counter=1;
        public  static  Map<Integer, Integer> map_of_jobId_to_num_of_map_started;
        public  static  Map<Integer, Integer> map_of_jobId_to_num_of_reduce_started;
        public  static  Map<String, Integer> map_of_filename_to_jobId;
        public  static  Map<Integer,String> map_of_jobId_to_filename;
        public  static  Queue queue_for_jobs ;
        public  static  Queue queue_for_tasks_of_job ;
        public  static  Queue queue_for_tasks_of_reducer_job ;
        public  static Map<Integer,new_job>map_for_jobId_to_job_detail; ;
	public static void main(String args[]) throws IOException
        {
         ////===INITIALISING=======HASHMAPS=================================
            queue_for_jobs = new LinkedList();
            queue_for_tasks_of_job = new LinkedList();
            queue_for_tasks_of_reducer_job = new LinkedList();
            map_of_filename_to_jobId = new HashMap<String,Integer>();
            map_of_jobId_to_filename = new HashMap<Integer,String>();
            map_for_jobId_to_job_detail = new HashMap<Integer,new_job>();
            map_of_jobId_to_num_of_map_started=new HashMap<Integer, Integer>();
             map_of_jobId_to_num_of_reduce_started=new HashMap<Integer, Integer>();
         ////====starting ========JT====SERVER============================== 
               try
               {
                   int x=Integer.parseInt(args[0]);
                    int port = 45000 +x;
                    System.setProperty("java.rmi.server.hostname", "10.0.0."+x);
                    job_tracker obj = new job_tracker();
                    IJobTracker stub = (IJobTracker)UnicastRemoteObject.exportObject((Remote) obj, port);
                    Registry registry = LocateRegistry.createRegistry(port);
                    registry.rebind("JT"+"10.0.0."+x, stub);
                    System.out.println("Server ready");	
                    
                    
                }
               catch (Exception e)
               {
			System.err.println("Some error in Rmi");
			e.printStackTrace();
		}
               
               
	
	}

    
        
        
    public  synchronized   int execute_job(String file,int x,String mapper_name) throws RemoteException, NotBoundException
    {
        System.out.println("job submitted  "+file);
        INameNode tempStub=get_object_of_name_node(file);
        return get_blok_location_including_open_file_and_add_to_task_queue(tempStub,file,x,mapper_name);
    }
    
    public synchronized  static   int  get_blok_location_including_open_file_and_add_to_task_queue(INameNode tempStub,String file,int job_id,String mapper_name) throws RemoteException
    {
        int num_of_block = 0;
        MyClient obj1=new MyClient();
        byte[] b=obj1.encode_open_file_request(file,true);
        b=tempStub.openFile(b);
        List<Integer> a=obj1.decode_open_file_response(b);
        int handle=a.get(a.size()-1);
        int statues=a.get(a.size()-2);
        a.remove(a.size()-1);a.remove(a.size()-1);
        try
        {
            String s="";
            for(int i=0;i<a.size()-1;i++)
            {
                s=s+a.get(i)+":";
            }
            s+=a.get(a.size()-1);
            String[] z=s.split(":");
            num_of_block=z.length;
            s=new String(tempStub.getBlockLocations(s.getBytes()));
            String[] block_loc=s.split(":");
            for(int i=0;i<block_loc.length;i++)
            {
                String ip=static_ip+block_loc[i];
                int port=55000+Integer.parseInt(block_loc[i]);
                DataNodeLocation obj2=new DataNodeLocation(ip,port);
                new_task_mapper obj=new new_task_mapper(job_id, i, mapper_name, Integer.parseInt(z[i]),obj2);
                queue_for_tasks_of_job.add(obj);
               
            }
        }
        catch(Exception e)
        {
            System.out.println("get error in get block location");
        }
       return num_of_block;
    }
    
   public  synchronized   static INameNode get_object_of_name_node(String file) throws RemoteException, NotBoundException
   {
       Registry registry=LocateRegistry.getRegistry("10.0.0.1",55000);
       INameNode tempStub = (INameNode) registry.lookup("NN"+static_ip+"1");
       return tempStub;
   }
   
    public  synchronized int jobSubmit(byte[] b) throws RemoteException 
    {
        String f=decode_job_submit(b);
       // System.out.println(f);
        String [] s=f.split(":");
        String mapper_name=s[0];//args[0];
        String reducer_name=s[1];
        String file_name=s[2];
        String output_file=s[3];
        int reducer_count=Integer.parseInt(s[4]);
        map_of_filename_to_jobId.put(file_name,job_counter);
        map_of_jobId_to_filename.put(job_counter, file_name);
        int x=job_counter;
        map_of_jobId_to_num_of_map_started.put(x, 0);
        map_of_jobId_to_num_of_reduce_started.put(x,0);
        int num_of_mappers=0;
        try
        {
              num_of_mappers=  execute_job(file_name,x,mapper_name);
              new_job obj=new new_job(mapper_name, reducer_name, file_name, output_file, reducer_count,num_of_mappers);
              map_for_jobId_to_job_detail.put(x, obj);
        }
        catch (NotBoundException ex) 
        {
                Logger.getLogger(job_tracker.class.getName()).log(Level.SEVERE, null, ex);
        }
        job_counter++;
        //System.out.println(new String(b));
        
        return x;//*/return 0;
    }

    
    public  synchronized   static String decode_job_submit(byte[] b)
    {
       // System.out.println(new String(b));
        return new String(b);
    }
    
    
    public synchronized    byte[] getJobStatus(byte[] b) throws RemoteException 
    {
        return encode_get_job_status(Integer.parseInt(new String(b)));
        
    }

    public  synchronized   static byte[] encode_get_job_status(int x)
    {
        String s="1:";
       // System.out.println("job id by client is "+x);
        boolean z=true;
       if(map_for_jobId_to_job_detail.get(x)==null)
       {
          s+=z;
          return s.getBytes();
       }
       
        new_job obj=map_for_jobId_to_job_detail.get(x);
        z=obj.com; 
        s+=z+":";
        s+=obj.num_of_mappers+":";
        System.out.println("response is "+obj.num_of_mappers);
        int x1=obj.num_of_mappers-obj.diff1;
        s+=x1+":";
        s+=obj.reducer_count+":";
        System.out.println("response is "+obj.reducer_count);
        s+=(obj.reducer_count-obj.diff);
        return s.getBytes();
    }
    
    public  synchronized   byte[] heartBeat(byte[] b) throws RemoteException 
    {
        int []a = null;
            try {
                a = decodeHeartBeatRequest(b);
            } catch (InvalidProtocolBufferException ex) {
                Logger.getLogger(job_tracker.class.getName()).log(Level.SEVERE, null, ex);
            }
        int free_mapper=a[0];
        int free_reducer=a[1];
        MapReduce.HeartBeatResponse.Builder res1=MapReduce.HeartBeatResponse.newBuilder();
        res1.setStatus(1);
        //System.out.println("heratbeat received");
        for(int i=0;i<free_mapper;i++)
        {
            if(!queue_for_tasks_of_job.isEmpty())
            { 
                System.out.println("encoding mapper");
                new_task_mapper obj=(new_task_mapper) queue_for_tasks_of_job.poll();
                MapReduce.MapTaskInfo.Builder req=MapReduce.MapTaskInfo.newBuilder();
                req.setJobId(obj.job_id);
                req.setTaskId(obj.task_id);
                req.setMapName(obj.mapper_name);
                MapReduce.BlockLocations.Builder res_datalocation =MapReduce.BlockLocations.newBuilder();
                
                MapReduce.DataNodeLocation.Builder re_datanode=MapReduce.DataNodeLocation.newBuilder();
                re_datanode.setIp(obj.obj.get_ip());
                re_datanode.setPort(obj.obj.get_port());
                
                res_datalocation.setBlockNumber(obj.block_num);
                res_datalocation.setLocations(re_datanode);
                
                req.setInputBlocks(res_datalocation);
                res1.addMapTasks(i, req);
                int c=map_of_jobId_to_num_of_map_started.get(obj.job_id);
                System.out.println((c+1)+" num of job started");
                map_of_jobId_to_num_of_map_started.put(obj.job_id, c+1);
            }
        }
        
        for(int i=0;i<free_reducer;i++)
        {
            if(!queue_for_tasks_of_reducer_job.isEmpty())
            { 
                System.out.println("encoding reducer");
                new_task_reducer obj=(new_task_reducer) queue_for_tasks_of_reducer_job.poll();
                MapReduce.ReducerTaskInfo.Builder req=MapReduce.ReducerTaskInfo.newBuilder();
                req.setJobId(obj.job_id);
                req.setTaskId(obj.task_id);
                req.setReducerName(obj.reducer_name);
                req.setOutputFile(obj.outputfile);
                for(int j=0;j<obj.map_output_file.length;j++)
                {
                    System.out.print("adding mapoutout files ");
                    System.out.println(obj.map_output_file[j]);
                    req.addMapOutputFiles( obj.map_output_file[j]);
                    
                }
                res1.addReduceTasks(req);
                int c=map_of_jobId_to_num_of_reduce_started.get(obj.job_id);
                System.out.println((c+1)+" num of reducer job started");
                map_of_jobId_to_num_of_map_started.put(obj.job_id, c+1);
            }
        }
        System.out.println(res1.build());
       
        return res1.build().toByteArray();
        
    }

    public  synchronized   static void insert_in_reducer_queue(int job_id,String out_file_name)
    {
        new_job obj=map_for_jobId_to_job_detail.get(job_id);
        //System.out.println("THIS IS REDUCER "+obj.num_of_mappers);
        int x=obj.num_of_mappers;
        //System.out.println(obj.reducer_count+"  insert in reducer num mapper "+x);
        int num=obj.reducer_count;
        int j=x/num;
        //System.out.println("j is "+j);
        int i=0;
        for(i=1;i<=num;i++)
        {
            int c=0,z=j;
            x-=j;
            int flag=0;
            
            if(x<j)
            { z+=x;flag=1;}
            String []s=new String [z];
            while(c<z)
            {
                
                s[c]=obj.map_task_output_file[c+j*(i-1)];
            //    System.out.println((i-1)+" recucer file for debug "+s[c]);
                c++;
            }
            new_task_reducer obj1=new new_task_reducer(job_id, i-1, obj.reducer_name, s,out_file_name);
            queue_for_tasks_of_reducer_job.add(obj1);
            if(flag==1)
                break;
        }
    }
    
    public synchronized    static int[] decodeHeartBeatRequest(byte[] arr) throws InvalidProtocolBufferException  
    {
        int num_of_mapper=0;
        int num_of_reducer=0;
        MapReduce.HeartBeatRequest req1=MapReduce.HeartBeatRequest.parseFrom(arr);
        try {
            
            num_of_mapper=req1.getNumMapSlotsFree();
            num_of_reducer=req1.getNumReduceSlotsFree();
            for(int i=0;i<req1.getMapStatusCount();i++)
            {
                MapReduce.MapTaskStatus req2=req1.getMapStatus(i);
                int job_id=req2.getJobId();
                new_job obj=map_for_jobId_to_job_detail.get(job_id);
                boolean completed=req2.getTaskCompleted();
                int x=obj.update(req2.getTaskId(), completed,req2.getMapOutputFile());
                if(x==1)
                {
                    insert_in_reducer_queue(job_id,obj.output_file);
                }
            }
        }
        catch (Exception ex) 
        {
            System.out.println("Exception in decode mapper heartbeat_request");
        }
        try{ 
            for(int i=0;i<req1.getReduceStatusCount();i++)
            {
                MapReduce.ReduceTaskStatus req3=req1.getReduceStatus(i);
                int job=req3.getJobId();
                int task=req3.getTaskId();
                boolean b=req3.getTaskCompleted();
                new_job obj=map_for_jobId_to_job_detail.get(job);
                int x=obj.update_reducer(task, b);
                if(x==1)
                {
                    map_for_jobId_to_job_detail.remove(job);
                    map_of_jobId_to_filename.remove(job);
                    
                }
            }
        } 
        catch (Exception ex) 
        {
            System.out.println("Exception in decode reducer heartbeat_request");
        }
        int []a={num_of_mapper,num_of_reducer};
        return a;
    }
    
}


