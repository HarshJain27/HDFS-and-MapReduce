

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class client 
{
    public static String jt_ip="";
    public static int jt_port;
    public static int status=0;
    public static void main(String[] args) 
    {
        
        int port = 55000+1;//Integer.parseInt(args[0]);//suffix of job tracker
        int job_id;
        String mapper_name=args[0];
        String reducer_name=args[1];
        String file_name=args[2];
        String output_file=args[3];
        int reducer_count=Integer.parseInt(args[4]);;
        
        try {
                read_con_file_for_jt_ip_port();
		Registry registry = LocateRegistry.getRegistry(jt_ip,jt_port);	
		IJobTracker tempStub = (IJobTracker) registry.lookup("JT"+jt_ip);
		System.out.println("CLIENT IS UP");
       //======CALLING===JOB===SUBMIT=======================
                byte[] b=encode_job_submit_request(mapper_name,reducer_name,file_name,output_file,reducer_count);
                job_id=tempStub.jobSubmit(b);
		System.out.println(job_id);
                System.out.println("Called method");
                String job=job_id+"";
                byte[] z=job.getBytes();
                while(status!=1)
                {
                    Thread.sleep(2000);
                    b=tempStub.getJobStatus(z);
                    decode_getjob_status(b);
                }
	    }
            catch (Exception e) 
            {
			System.err.println("Some error in Rmi");
			e.printStackTrace();
            }
    }
    
    public static void decode_getjob_status(byte[] b)
    {
        String [] s=new String (b).split(":");
        if(Boolean.parseBoolean(s[1])==true)
        {
            System.out.println("file mapped and reduced task completed");
            status=1;
            
            return;
        }
        //System.out.println(new String (b));
        System.out.println("task complete:"+s[1]);
        System.out.println("number of mappers :"+s[2]);
        System.out.println("number of mapper started "+s[3]);
        System.out.println("number of reducers "+s[4]);
        System.out.println("number of reducer started :"+s[5]);
        float x=Float.parseFloat(s[3])/Float.parseFloat(s[2]);
        System.out.println("map  task completed"+x);
        x=Float.parseFloat(s[5])/Float.parseFloat(s[4]);
        System.out.println("reducer  task completed"+x);
    }
    
    public static byte[] encode_job_submit_request(String mapper_name,String reducer_name,String file_name,String output_file,int reducer_count)
    {
        String s=mapper_name+":"+reducer_name+":"+file_name+":"+output_file+":"+reducer_count;
        return s.getBytes();
    }
    
    public static void read_con_file_for_jt_ip_port()
    {
            File f = new File("configuration_file.txt");
            try {
		    FileReader fileReader = new FileReader(f);
                    BufferedReader bufferedReader = new BufferedReader(fileReader);
                    StringBuffer stringBuffer = new StringBuffer();
                    String line;
                    line = bufferedReader.readLine();
                    fileReader.close();
                    System.out.println("Contents of file:");
                    System.out.println(line);
                    String [] s=line.split(":");
                    jt_ip=s[0];
                    jt_port=Integer.parseInt(s[1]);
		} 
                catch (Exception e)
                {
                    System.out.println("Exception in config file read");
			e.printStackTrace();
		}
    }
}
