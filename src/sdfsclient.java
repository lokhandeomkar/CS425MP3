import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;


public class sdfsclient {
	public static void main(String args[]) throws UnknownHostException, IOException 
	{
		String masterIP = args[0];
		Socket socket;
		byte[] msgdata = new byte[8192];
		byte[] filedata = new byte[8192];
		String sdfsfilename, localfilename;
		int count;
		BufferedReader inputStream = new BufferedReader(new InputStreamReader(System.in));
		BufferedInputStream fromFile = null;
		BufferedOutputStream toFile = null;
		BufferedInputStream fromMaster = null;
		BufferedOutputStream toMaster = null;
		BufferedInputStream fromMachine = null;
		BufferedOutputStream toMachine = null;
		int dfssocket = 40000;
		int incomingport = 42000;


		String userCmd;
		System.out.println("Connecting to master...");
		System.out.print("Enter cmd: ");
		while((userCmd=inputStream.readLine())!=null)
		{
			try 
			{
				socket = new Socket(masterIP,dfssocket);
				toMaster = new BufferedOutputStream(socket.getOutputStream());
				fromMaster = new BufferedInputStream(socket.getInputStream());
				if(userCmd.equals("M"))
				{
					System.out.println("\nEnter new master ip: ");
					userCmd=inputStream.readLine();
					masterIP = userCmd;
					System.out.println("\nMaster has been updated.");
					System.out.print("\nEnter cmd (M to change master): ");
				}
				else
				{
					String[] usercmdarray = userCmd.split(" ");
					if (usercmdarray[0].equals("put"))
					{
						sdfsfilename = usercmdarray[2].trim();
						localfilename = usercmdarray[1].trim();

						String newusercmd = "put "+sdfsfilename;	
						msgdata=newusercmd.getBytes();
						filedata =  new byte[8192];
						filedata[0] = (byte) newusercmd.length();
						System.arraycopy(msgdata, 0, filedata, 1, msgdata.length);
						fromFile = new BufferedInputStream(new FileInputStream(localfilename));

						toMaster.write(filedata,0,filedata.length);
						System.out.print("Transferring file ...");

						while ((count = fromFile.read(filedata)) > 0) 
						{
							toMaster.write(filedata, 0, count);
						}
						toMaster.flush();
						fromFile.close();
						System.out.print(" DONE");
						System.out.print("\nEnter cmd (M to change master): ");
					}
					else if (usercmdarray[0].equals("delete"))
					{
						sdfsfilename = usercmdarray[1].trim();
						String newusercmd = "delete "+sdfsfilename;
						msgdata=newusercmd.getBytes();
						filedata =  new byte[8192];
						filedata[0] = (byte) newusercmd.length();
						System.arraycopy(msgdata, 0, filedata, 1, msgdata.length);
						toMaster.write(filedata,0,filedata.length);
						toMaster.flush();
						System.out.print("Delete message sent");
						System.out.print("\nEnter cmd (M to change master): ");
					}
					else if (usercmdarray[0].equals("get"))
					{
						sdfsfilename = usercmdarray[1].trim();
						localfilename = usercmdarray[2].trim();

						String newusercmd = "get "+sdfsfilename;
						msgdata=newusercmd.getBytes();
						filedata =  new byte[8192];
						filedata[0] = (byte) newusercmd.length();
						System.arraycopy(msgdata, 0, filedata, 1, msgdata.length);
						toMaster.write(filedata,0,filedata.length);

						System.out.print("Waiting for replicaservers from Master: ");
						fromMaster.read(filedata);
						String replica = new String(filedata,1,filedata[0]);
						System.out.print(replica+"\n");
						socket.close();
						if (replica.equals("FileNotFound"))
						{
							System.out.print("File not found.");
							System.out.print("\nEnter cmd (M to change master): ");
							continue;
						}
						String[] repmachines = replica.split("\t");
						ServerSocket incomingsocket = new ServerSocket(incomingport);
						for (String currentMachine:repmachines)
						{	
							System.out.print("Contacting "+currentMachine+".");
							try
							{
								socket = new Socket(currentMachine,dfssocket);
								toMachine = new BufferedOutputStream(socket.getOutputStream());
								newusercmd = "getfs "+sdfsfilename+" "+Integer.toString(incomingport);
								msgdata=newusercmd.getBytes();
								filedata =  new byte[8192];
								filedata[0] = (byte) newusercmd.length();
								System.arraycopy(msgdata, 0, filedata, 1, msgdata.length);
								toMachine.write(filedata,0,filedata.length);
								toMachine.close();
								Socket ftsocket = incomingsocket.accept();
								BufferedInputStream ftstream = new BufferedInputStream(ftsocket.getInputStream());
								toFile = new BufferedOutputStream(new FileOutputStream(localfilename));
								int totalbytes = 0;
								boolean firstpacket = true;
								while((count = ftstream.read(filedata))>0)
								{	
									if(firstpacket==true)
									{
										firstpacket =false;
										continue;
									}
									else
									{
										totalbytes = totalbytes+count;
										toFile.write(filedata, 0, count);
									}
								}
								toFile.close();
								System.out.println("File received, written "+totalbytes+"B");
								System.out.print("\nEnter cmd (M to change master): ");
								break;
							}
							catch (IOException e)
							{
								System.out.println("Error connecting to "+currentMachine);
								continue;
							}

						}
					}
				}
				if(socket!=null)socket.close();
			}
			catch (IOException e) {
				e.printStackTrace();
			} 
		}
	}
}