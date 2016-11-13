import java.rmi.Naming;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.io.*;
import java.util.*;
import com.bagl.protobuf.Hdfs.*;
import com.google.protobuf.ByteString;
public class Client extends UnicastRemoteObject
{	
	static String nn_host = "127.0.0.1";	
	static INameNode namenode;
	static int BLOCK_SIZE = 32000000;
	public Client() throws RemoteException {}
        static String jt_host = "127.0.0.1";
	static ArrayList<Integer> job_list = new ArrayList<Integer>();
	public static int open_file(String fname, String op){
		OpenFileRequest.Builder openfile = OpenFileRequest.newBuilder();
		OpenFileResponse resp = null;
		openfile.setFileName(fname);
		if (op.equals("w"))
			openfile.setForRead(false);
		else
			openfile.setForRead(true);
		OpenFileRequest write_req = openfile.build();
		try{
		resp = OpenFileResponse.parseFrom(namenode.openFile(write_req.toByteArray()));
		System.out.println("Handle obtained: " + resp.getHandle() + "");
		}
		catch (Exception e){
			System.out.println("Error opening file" + e.getMessage());
			e.printStackTrace();
			return -1;
		}
		return resp.getHandle();
		
	}
	public static void list_files(String fname)
         {
                try{
                 ListFilesRequest.Builder lfr = ListFilesRequest.newBuilder();
                 lfr.setDirName(fname);
                 byte[] temp = namenode.list(lfr.build().toByteArray());
                 ListFilesResponse lfr_response = ListFilesResponse.parseFrom(temp);
                 if(lfr_response.getStatus() < 0)
                 {
                        System.out.println("Error: Bad File List response recieved");
                 }
                 else{
                        for(String file_name : lfr_response.getFileNamesList())
                        {
                                System.out.println("File Found: " + file_name);
                        }
                 }
             	}
                catch(Exception e)
                {
                        System.out.println("Error: Could not list files" + e.getMessage());
			e.printStackTrace();
                }

         }
	public static void make_chunks(int handle, String fname){
		String FILE_NAME = fname;
		if (handle < 0){
			System.out.println("Error: Opening the File for write");
			return;
			}
		File inputFile = new File(FILE_NAME);
		FileInputStream inputStream;
		String newFileName;
		int fileSize = (int) inputFile.length();
		int nChunks = 0, read = 0, readLength = BLOCK_SIZE;
		byte[] byteChunkPart;
		try {
			inputStream = new FileInputStream(inputFile);
			ArrayList<Integer> temp = new ArrayList<Integer>();
			while (fileSize > 0) {
				if (fileSize <= BLOCK_SIZE) {
					readLength = fileSize;
				}
				byteChunkPart = new byte[readLength];
				read = inputStream.read(byteChunkPart, 0, readLength);
				fileSize -= read;
				assert (read == byteChunkPart.length);
				nChunks++;
				AssignBlockRequest.Builder assignreq = AssignBlockRequest.newBuilder();
				assignreq.setHandle(handle);
				AssignBlockResponse assignresp = AssignBlockResponse.parseFrom(namenode.assignBlock(assignreq.build().toByteArray()));
				if (assignresp.getStatus() < 0)
					System.out.println("Error Allocating DataNode Locations");
				else{
                 	WriteBlockRequest.Builder write_req = WriteBlockRequest.newBuilder();
		 			write_req.addData(ByteString.copyFrom(byteChunkPart));
                    write_req.setBlockInfo(assignresp.getNewBlock()); 
					System.out.println("Recieved Block Allocations for: " + assignresp.getNewBlock().getBlockNumber());
					temp.add(assignresp.getNewBlock().getBlockNumber());
					for(DataNodeLocation dnode : assignresp.getNewBlock().getLocationsList()){
						System.out.println("Sending Blocks to DataNode " + dnode.getIp() + ":" + dnode.getPort());
                        IDataNode datanode = (IDataNode)Naming.lookup("//" + dnode.getIp() + "/DataNode");
                        WriteBlockResponse resp = WriteBlockResponse.parseFrom(datanode.writeBlock(write_req.build().toByteArray()));
                        if (resp.getStatus() < 0)
                            System.out.println("Error: Could not send Block Information to DataNode@: " + dnode.getIp());
                        else
                            System.out.println("Successfully Sent Blocks to DataNode@: " + dnode.getIp());
					}
				}
				byteChunkPart = null;
			}

            CloseFileRequest.Builder closereq = CloseFileRequest.newBuilder();
            closereq.setHandle(handle);
            CloseFileResponse closeresp = CloseFileResponse.parseFrom(namenode.closeFile(closereq.build().toByteArray()));
            if (closeresp.getStatus() < 0)
                System.out.println("Error: Could not close the file");
            else
                System.out.println("File Successfully Closed after writing");

	/*		BufferedWriter out = null;
	                FileWriter fstream = new FileWriter("file_config.txt",true); //true tells to append data.
        	        out = new BufferedWriter(fstream);
			out.write(inputFile+"\n");
			out.write(String.valueOf(temp.size())+"\n");
			int l = 0;
			for(l = 0; l < temp.size();l++)
			{
				out.write(String.valueOf(temp.get(l))+"\n");
			}
			out.close();
			inputStream.close();*/
			return;
		} catch (Exception e) {
			System.out.println("Error: Could not write File to HDFS: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public static byte[] getBlockLocations(int[] array)
         {
             try{
                 BlockLocationRequest.Builder blc = BlockLocationRequest.newBuilder();
                 for(int i=0;i<array.length;i++)
                 {
                        blc.addBlockNums(array[i]);
                 }
                 return namenode.getBlockLocations(blc.build().toByteArray());
             }
             catch (Exception e)
             {
                System.out.println("Error: Something went bad while recieving Block Locations: " + e.getMessage());
		e.printStackTrace();
             }
	    return new byte[] {1};
         }	
	
	public static void getFile(String file_name)
         {
		try{
		BufferedReader br = new BufferedReader(new FileReader("file_config"));
		String line;
		int status = 0;
		int num_blocks = 0;
		int[] block_list = new int[1];
		while ((line = br.readLine()) != null) {
		if(status == 2)
		{
			block_list[(block_list.length)-num_blocks] = Integer.parseInt(line.split(" ")[0]);
			num_blocks = num_blocks-1;
			if(num_blocks==0)
			{
				break;
			}
		}
		if(status == 1)
		{
			num_blocks = Integer.parseInt(line.split(" ")[0]);
			block_list = new int[num_blocks];
			status = 2;
		}
       		if(line.equals(file_name))
		{
			System.out.println("Found file: " + file_name);
			status = 1;			
		}	
    		}
		BufferedWriter out = null;
    		FileWriter fstream = new FileWriter(file_name,true); //true tells to append data.
    		out = new BufferedWriter(fstream);
 		BlockLocationResponse blc_response = BlockLocationResponse.parseFrom(getBlockLocations(block_list));
		for(BlockLocations block: blc_response.getBlockLocationsList())
                 {
                        for(DataNodeLocation dnc: block.getLocationsList())
                        { 
                		IDataNode obj = (IDataNode)Naming.lookup("//" +dnc.getIp() + "/DataNode");
                		ReadBlockRequest.Builder rbr = ReadBlockRequest.newBuilder();
                		rbr.setBlockNumber(block.getBlockNumber());
				
                		ReadBlockResponse read_resp = ReadBlockResponse.parseFrom(obj.readBlock(rbr.build().toByteArray()));
				if (read_resp.getStatus() < 0)
					System.out.println("Error: Something went Bad while fetching files from HDFS");
				else{
				out.write(new String(read_resp.getData(0).toByteArray(), "UTF-8"));
				}
			}
		}
		out.close();
		}
		catch(Exception e){
			System.out.println("Eror: Something went wrong while reading HDFS file: " + e.getMessage());
			e.printStackTrace();
		}

        }
	public static void main(String args[]){
		try{	
			job_submit("file1","file2","file3","file4",5);
			namenode = (INameNode)Naming.lookup("//" + nn_host + "/NameNode");
			Client obj = new Client();
			Naming.rebind("Client", obj);
			while(true){
			InputStreamReader ISR = new InputStreamReader(System.in);
        		BufferedReader br = new BufferedReader(ISR);
       			System.out.print("bagl@HDFSFileStorage$: ");
        		String cmd = br.readLine();
			if (cmd.contains("Put")){
				String fname = cmd.split(" ")[1];
				int handle = open_file(fname,"w");
				make_chunks(handle, fname);
			}
			else if (cmd.contains("Get")){
				String fname = cmd.split(" ")[1];
				open_file(fname,"r");
				getFile(fname);
			}
			else if (cmd.contains("List")){
				String fname = cmd.split(" ")[1];
				list_files(fname);
			}
			}
		}
		catch (Exception e){
			System.out.println("err" + e.getMessage());
			e.printStackTrace();
		}

	}
	public static void job_submit(String mapper, String reducer, String input, String output, Integer num_tasks)
	{
		try{
			JobSubmitRequest.Builder jsr = JobSubmitRequest.newBuilder();
			jsr.setMapName(mapper);
			jsr.setReducerName(reducer);
			jsr.setInputFile(input);
			jsr.setOutputFile(output);
			jsr.setNumReduceTasks(num_tasks);
			IJobTracker jobtracker = (IJobTracker)Naming.lookup("//" + jt_host + "/JobTracker");		
			JobSubmitResponse jsr_response = JobSubmitResponse.parseFrom(jobtracker.jobSubmit(jsr.build().toByteArray()));
			if(jsr_response.getStatus() == 0)
			{
				System.out.println("Something bad happened while making the job request");
			}
			else
			{
				int job_id = jsr_response.getJobId();
				System.out.println("This is the id of the submitted job : " + job_id);
				job_list.add(job_id);				
				new job_status_loop().start();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
			
	}
        public static class job_status_loop extends Thread{
		public void job_status_loop(){}
		public void run()
		{
			try
			{
				int job_id = job_list.get(job_list.size()-1);
				while(true)
				{
					JobStatusRequest.Builder jsr = JobStatusRequest.newBuilder();
					jsr.setJobId(job_id);
					IJobTracker jobtracker = (IJobTracker)Naming.lookup("//" + jt_host + "/JobTracker");
					JobStatusResponse jsr_response = JobStatusResponse.parseFrom(jobtracker.getJobStatus(jsr.build().toByteArray()));
					if(jsr_response.getStatus()==0)
					{
						System.out.println("Something bad happened");
					}
					else	
					{
						if(jsr_response.getJobDone())
						{
							System.out.println("The job is finished ");
							break;
						}
						else
						{
							System.out.println("The job is still going on");		
						}			
					}
					Thread.sleep(1000);
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}

}

