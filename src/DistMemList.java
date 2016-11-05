/*
Five threads were used, two for gossip sending and receiving and two for 
election sending and receiving. The last one for file operations. 

UDP was used for gossipping and TCP for election and file operations.
*/

import java.io.*;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class DistMemList implements Runnable
{
	volatile Map<String,String> ownmemList = new HashMap<String,String>();
	volatile Queue<String> incomingMessageBuffer = new LinkedList<String>();
	volatile Queue<String> incomingElectionMsgBuffer = new LinkedList<String>();
	Map<String,List<String>> masterFileList = new HashMap<String,List<String>>();
	List<String> localFileList = new ArrayList<String>();

	volatile long heartBeatSeq = 1;

	String machineID;
	String selfAddress;
	String selfUpdateTime;
	String currSysStatus;
	int listeningport;
	int TfailinMS;
	int TcleaninMS;
	int gossipTimeinMS;
	int msg_sent;
	int msg_rec;
	int byte_sent;
	int byte_rec;
	double drop_rate;
	String masterName;
	boolean ongoingElection;
	boolean displayFlag;
	boolean newGroupMember;
	int fileopsport = 40000;

	static Logger logger = Logger.getLogger(DistMemList.class.getName());

	public DistMemList(String setIntroducerName,int setlisteningport,int setTfailinMS,int setTcleaninMS,int setgossipTimeinMS,double setdroprate){
		this.listeningport = setlisteningport;
		this.TfailinMS = setTfailinMS;
		this.TcleaninMS = setTcleaninMS;
		this.gossipTimeinMS = setgossipTimeinMS;
		this.masterName = setIntroducerName;
		this.currSysStatus = "W";
		this.drop_rate = setdroprate;
		this.ongoingElection = false;
		this.newGroupMember = true;
		displayFlag = false;
		machineID = "";
		selfAddress = null;
		// Add self entry to the membership list
		Date currDate = new Date();
		Timestamp currTime = new Timestamp(currDate.getTime());
		SimpleDateFormat mlformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String mltimeStamp = mlformat.format(currTime);
		try{
			InetAddress localAddress = InetAddress.getLocalHost();
			selfAddress = localAddress.getHostAddress();
			machineID = selfAddress+" "+mltimeStamp;
		}
		catch (UnknownHostException uhe)
		{
			System.out.println("ERROR: unable to lookup self ip address");
			System.exit(0);
		}
		PropertyConfigurator.configure("./log4j.properties");		
		logger.info("[I] Starting Daemon");
	}


	public void sender() throws IOException{
		ownmemList = new HashMap<String,String>();
		heartBeatSeq = 0;

		String masterIP = masterName;
		InetAddress memberAddress;

		int RunningTime = 3000;
		int Kgossip = 1;
		int jGossipMachines;

		//		Date currDate = new Date();
		//		Timestamp currTime = new Timestamp(currDate.getTime());
		SimpleDateFormat mlformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

		//create variables to send data
		DatagramSocket outgoingSocket = new DatagramSocket();
		DatagramPacket sendPacket;
		InetAddress masterAddress = InetAddress.getByName(masterIP);
		byte[] sendData = new byte[1024];
		String sendListLine = null;

		//keep track of the time to regulate gossiping
		Date date = new Date();
		Timestamp prevtime = new Timestamp(date.getTime());
		Timestamp currtime = prevtime;
		String currtimestring = mlformat.format(currtime);
		selfUpdateTime = currtimestring;

		Set<String> keys = ownmemList.keySet();
		List<String> allKeys = new ArrayList<String>();
		String message;

		//if the current machine is the introducer, simply wait for incoming messages, else keep pinging introducer	
		if (selfAddress.equals(masterAddress.getHostAddress()))
		{
			do{
				if ((message = incomingMessageBuffer.poll()) != null)
				{
					list_merge(message);
				}
				keys = ownmemList.keySet();
			}
			while (keys.size() < 1);
		}
		else
		{
			do{
				date = new Date();
				currtime = new Timestamp(date.getTime());
				currtimestring = mlformat.format(currtime);
				if ((currtime.getTime()-prevtime.getTime()) >= gossipTimeinMS)
				{
					prevtime = currtime;
					sendListLine = "";
					sendListLine = machineID +"\t"+Long.toString(heartBeatSeq)+"\t"+currtimestring+"\tW\n";
					sendData = sendListLine.getBytes();
					sendPacket = new DatagramPacket(sendData,sendData.length,masterAddress,listeningport);
					try {
						outgoingSocket.send(sendPacket);
						msg_sent++; // added by Omkar
						byte_sent +=  sendPacket.getLength();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if ((message = incomingMessageBuffer.poll()) != null)
				{
					list_merge(message);
				}
				keys = ownmemList.keySet();
			}
			while (keys.size() < 1);
			newGroupMember = false;
		}

		//keep sending the selfmember list to introducer till one member of the group gossips back


		//new member(s) has joined
		keys = ownmemList.keySet();

		// there are atleast 1 additional entry in the membership list, which means we have joined the group		
		// now we can start normal gossiping, by selecting k members randomly to send the message to
		date = new Date();
		currtime = new Timestamp(date.getTime());
		prevtime = currtime;
		Timestamp prevDisplaytime = currtime;

		Timestamp t0 = currtime;

		while((currtime.getTime()-t0.getTime())*1E-3 <= RunningTime)
		{
			allKeys.addAll(keys);

			// find out current time and check if Tgossip milliseconds have passed			
			date = new Date();
			currtime = new Timestamp(date.getTime());
			selfUpdateTime = mlformat.format(currtime);

			check_timeout();
			if ((message = incomingMessageBuffer.poll()) != null)
			{
				list_merge(message);
			}

			if ((currtime.getTime()-prevtime.getTime()) >= gossipTimeinMS)
			{
				prevtime = currtime;

				jGossipMachines=0;
				Collections.shuffle(allKeys);		

				sendListLine = get_list(selfUpdateTime);
				sendData = sendListLine.getBytes();

				while ((jGossipMachines < Kgossip) && (allKeys.size()>=1))
				{
					String memberID = allKeys.get(jGossipMachines);

					// using membership id get the ip address of the K targets
					String[] memberInfo = memberID.split(" ");
					String memberIP = memberInfo[0];
					memberAddress = InetAddress.getByName(memberIP);
					sendPacket = new DatagramPacket(sendData,sendData.length,memberAddress,listeningport);
					outgoingSocket.send(sendPacket);
					msg_sent++;
					byte_sent +=  sendPacket.getLength();

					jGossipMachines++;
				}				
				heartBeatSeq++;
			}
			if (displayFlag && ((currtime.getTime()-prevDisplaytime.getTime()) >= gossipTimeinMS)) // gossipTimeinMS
			{
				prevDisplaytime = currtime;
				System.out.println("CurrentMaster: "+masterName);
				System.out.println(sendListLine);
			}

			allKeys.clear();
			keys = ownmemList.keySet();			
		}
		outgoingSocket.close();
	}
	public void tcp_send(String destinationIP, String backupIP, String tcpmessage)
	{
		//setup a connection to the successor, check for failed connections
		try 
		{
			Socket client = new Socket(destinationIP, listeningport);
			PrintWriter out = new PrintWriter(client.getOutputStream(), true);
			out.println(tcpmessage);
			System.out.println(destinationIP+":"+tcpmessage);
		}
		catch(IOException e)
		{	
			System.out.println("ERROR connecting to server: "+destinationIP+":"+Integer.toString(listeningport));
			try (
					Socket client = new Socket(backupIP, listeningport);
					PrintWriter out = new PrintWriter(client.getOutputStream(), true);
					){
				out.println(tcpmessage);
				System.out.println(backupIP+":"+tcpmessage);
			}
			catch(IOException e2)
			{	
				System.out.println("ERROR connecting to server: "+destinationIP+":"+Integer.toString(listeningport));

			}
		}
	}

	public boolean checkMasterAlive()
	{
		boolean masterAlive = false;
		String sendListLine = get_list(selfUpdateTime);

		String[] memberentries = sendListLine.split("\n");
		for (String memberinfo: memberentries)
		{
			String[] memberinfoarray = memberinfo.split("\t");
			String[] memberlabel = memberinfoarray[0].split(" ");
			if(memberlabel[0].equals(masterName))
				masterAlive = true;
		}
		return masterAlive;
	}

	public void election_send() throws IOException
	{
		Set<String> keys =null;
		Date date = new Date();
		Timestamp currtime = new Timestamp(date.getTime());
		Timestamp prevtime = currtime;
		Timestamp prevelectrigger = currtime;

		int self_index, succ_index;
		String[] machineIDArray = new String[3];		
		String succ_IP, succ_IP_2;
		machineIDArray = machineID.split(" ");

		String electionMsg,electedMsg;

		//		boolean ;
		String lowestInitiator = null;

		do {
			date = new Date();
			currtime = new Timestamp(date.getTime());
			if ((currtime.getTime()-prevtime.getTime()) >= gossipTimeinMS)
			{
				prevtime=currtime;
				keys = ownmemList.keySet();

				if ((!newGroupMember))
				{

					InetAddress localAddress = InetAddress.getLocalHost();
					String selfAddress = localAddress.getHostAddress();
					InetAddress masterAddress = InetAddress.getByName(masterName);

					List<String> Ring = RingMaker();					

					self_index= Ring.indexOf(machineIDArray[0]);
					succ_index = (self_index+1)%(Ring.size());
					succ_IP = Ring.get(succ_index);
					succ_IP_2 =  Ring.get((succ_index+1)%(Ring.size()));

					if ((currtime.getTime() - prevelectrigger.getTime()) >= 16* gossipTimeinMS)
					{
						ongoingElection = false;
					}

					/*
					if (!checkMasterAlive() && (!ongoingElection))
					{
						try 
						{
						    Thread.sleep(3*gossipTimeinMS); // do not trigger immediately
						} 
						catch(InterruptedException ex) 
						{
						    Thread.currentThread().interrupt();
						}						
					}
					*/

					if (!checkMasterAlive() && (!ongoingElection))
					{
						ongoingElection = true;
						prevelectrigger = currtime;
						electionMsg = "election\t"+machineIDArray[0]+"\t"+machineIDArray[0];

						System.out.println("Election triggered by: "+ machineIDArray[0]);
						lowestInitiator = machineIDArray[0];
						tcp_send(succ_IP,succ_IP_2,electionMsg);						
					}

					while (!incomingElectionMsgBuffer.isEmpty())
					{
						String inelectMsg = incomingElectionMsgBuffer.poll();


						String[] inelectMsgArray = inelectMsg.split("\t"); 
						String incomingInitiator = inelectMsgArray[1];
						System.out.println("Received: "+inelectMsg);
						if (lowestInitiator == null)
						{						
							System.out.println("lowinit: null, incoming: "+incomingInitiator);
						}
						else
						{						
							System.out.println("lowinit: "+lowestInitiator+", incoming: "+incomingInitiator);
						}

						if (inelectMsgArray[0].equals("election"))
						{
							ongoingElection = true;
							String incomingAttribute = inelectMsgArray[2];
							if (lowestInitiator == null)
							{
								lowestInitiator = incomingInitiator;
							}
							if (incomingAttribute.compareTo(machineIDArray[0]) < 0)
							{
								//								if (incomingInitiator.compareTo(lowestInitiator) <= 0)
								//								{
								electionMsg = "election\t"+incomingInitiator+"\t"+incomingAttribute;

								tcp_send(succ_IP,succ_IP_2,electionMsg);
								lowestInitiator = incomingInitiator;
								//								}								
							}
							else if (incomingAttribute.compareTo(machineIDArray[0]) == 0)
							{
								masterName = selfAddress;
								electedMsg = "elected\t"+machineIDArray[0];
								tcp_send(succ_IP,succ_IP_2,electedMsg);
							}
							else
							{
								electionMsg = "election\t"+incomingInitiator+"\t"+machineIDArray[0];

								tcp_send(succ_IP,succ_IP_2,electionMsg);
							}
						}
						else if (inelectMsgArray[0].equals("elected"))
						{
							masterName = incomingInitiator;
							ongoingElection = false;
							lowestInitiator = null;
							if (!(masterName.equals(selfAddress)))
							{
								String listString = selfAddress;
								for (String s : localFileList)
								{
								    listString += " " + s;
								}
								// take off the last space character 
								//if (!listString.isEmpty())
								//{
								//	listString = listString.substring(0, listString.length()-1);
								//}
								electedMsg = inelectMsg+"\t"+listString;
								tcp_send(succ_IP,succ_IP_2,electedMsg);
							}
							// get the file lists sent by the group members, you are the new leader 
							else 
							{ // process the inelectMsgArray
								masterFileList = null; 
								// this may not have come through all the members ????
								List<String> filenames = new ArrayList<String>(localFileList);
								for (int i = 2; i < inelectMsgArray.length; i++)
								{
										String [] memberIDfilelist = inelectMsgArray[i].split(" ");
										for (int j = 1; j < memberIDfilelist.length; j++)
										{
											if (!filenames.contains(memberIDfilelist[j]))
											{
												filenames.add(memberIDfilelist[j]);
											}
										}
								}
								// you have all the filenames ????
								for (String fname : filenames)
								{
									List<String> machineIPS = new ArrayList<String>();									
									if (localFileList.contains(fname))
									{
										if (!machineIPS.contains(selfAddress))
											machineIPS.add(selfAddress);
									}
									for (int i = 2; i < inelectMsgArray.length; i++)
									{
										String [] memberIDfilelist = inelectMsgArray[i].split(" ");
										List<String> memberfilelist = new ArrayList<String>();
										for (int j = 1; j < memberIDfilelist.length; j++)
										{
											if (!memberfilelist.contains(memberIDfilelist[j]))
												memberfilelist.add(memberIDfilelist[j]);
										}
										if (memberfilelist.contains(fname))
										{
											if (!machineIPS.contains(memberIDfilelist[0]))
												machineIPS.add(memberIDfilelist[0]);	
										}	
									}
									masterFileList.put(fname,machineIPS);
									run_stabilization();	
								}
								System.out.println("masterFileList after the  new election");
								System.out.println(masterFileList);
							}	
						}
					}
				}	
			}
		}
		while(true);
	}


	public int stringtoHashedInteger(String s)
	{
		int hashedValue=0;
		String sha1Hash, subsha1;

		MessageDigest digest;
		try {
			digest = MessageDigest.getInstance("SHA-1");
			byte[] hashedBytes = digest.digest(s.getBytes("UTF-8"));
			sha1Hash = convertByteArrayToHexString(hashedBytes);
			subsha1= sha1Hash.substring(sha1Hash.length()-2);
			hashedValue = Integer.parseInt(subsha1,16);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return hashedValue; 
	}

	// code from codejava.net
	private static String convertByteArrayToHexString(byte[] arrayBytes) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < arrayBytes.length; i++) {
			stringBuffer.append(Integer.toString((arrayBytes[i] & 0xff) + 0x100, 16)
					.substring(1));
		}		
		return stringBuffer.toString();
	}

	/*
	Machine names are hashed. 
	*/
	
	public List<String> hashedRingMaker()
	{
		List<String> ring = new ArrayList<String>();
		List<String> hashedRing = new ArrayList<String>();
		int hashedNumber;

		String[] machineIDArray = new String[3];
		machineIDArray = machineID.split(" ");
		ring.add(machineIDArray[0]);

		Set<String> keys = ownmemList.keySet();		
		if (keys.size() > 0)
		{
			for (String key: keys)
			{
				machineIDArray = key.split(" ");
				ring.add(machineIDArray[0]);
			}
		}

		for (String machineName:ring)
		{
			hashedNumber = stringtoHashedInteger(machineName);
			String hashedString = String.format("%03d", hashedNumber); 
			hashedRing.add(hashedString+"\t"+machineName);
		}
		Collections.sort(hashedRing);		
		return hashedRing;
	}

	/*
	Puts the machine names in a list and sorts it.
	*/
	public List<String> RingMaker ()
	{
		List<String> ring = new ArrayList<String>();

		String[] machineIDArray = new String[3];
		machineIDArray = machineID.split(" ");
		ring.add(machineIDArray[0]);

		Set<String> keys = ownmemList.keySet();		
		if (keys.size() > 0)
		{
			for (String key: keys)
			{
				machineIDArray = key.split(" ");
				ring.add(machineIDArray[0]);
			}
		}	
		Collections.sort(ring);		
		return ring;
	}
	
	/*
	Gets the member list in a string for gossipping.
	*/

	public String get_list(String currtimestring)
	{
		String entireList = "";
		String sendListLine;
		//		entireList = "";
		sendListLine = machineID +"\t"+Long.toString(heartBeatSeq)+"\t"+currtimestring+"\t"+currSysStatus+"\n";
		entireList = sendListLine;

		Set<String> keys = ownmemList.keySet();
		if (keys.size() > 0)
		{
			for (String key: keys)
			{
				sendListLine = "";
				sendListLine = key+"\t"+ownmemList.get(key)+"\n";
				entireList = entireList+sendListLine;	
				//			entireList = "a";
			}
		}
		return entireList;
	}

	public void list_merge(String inlist)
	{
		// This function merges the lists

		// Process every entry from that list 
		// If self ID, goto a line, at that line, update the hb to the current global hb and update the time
		// If a new ID is there, copy that entry but with your current timestamp 
		// If the incoming heartbeat count is newer, update that entry with your current time and the newer heartbeat count
		// else Change status to 'F' if the heartbeat is not new and the process has timed out, update the timestamp
		// If the status is 'V', mark the process as 'F', if the process was marked failed, mark it as alive, in case of new hb

		// find out the local machine time
		Date currDate = new Date();
		Timestamp currTime = new Timestamp(currDate.getTime());
		SimpleDateFormat mlformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String currtimestring = mlformat.format(currTime);

		String[] listLines = inlist.split("\n"); // members are separated by \n

		for (String member: listLines) // processing every member
		{	
			String[] memberinfo = new String[4];			

			memberinfo = member.split("\t"); // tab separation

			String memberID = memberinfo[0];
			String memberHeartBeatString = memberinfo[1];
			Long memberHeartBeatSeq = Long.parseLong(memberHeartBeatString);
			String memberstatus = memberinfo[3];
			String newval = ""; 

			// check if this entry exists in own membership list - if not then add it with current time	 	  
			if ((!ownmemList.containsKey(memberID)) && !memberID.equals(machineID))
			{
				if(memberstatus.equals("W"))
				{
					newval = Long.toString(memberHeartBeatSeq)+"\t"+currtimestring+"\t"+memberstatus;
					ownmemList.put(memberID,newval);	

					run_stabilization();	

					logger.info("[New] "+memberID+"\t"+newval);
				}
			}
			// if the entry exists then, check the incoming heart beat sequence, and if it is greater than ownlist seq update it with current time
			else //this is for those entries which are already present in the membership list and not equal to the localmachine
			{
				if (!memberID.equals(machineID))
				{
					String currValue = ownmemList.get(memberID);
					String[] ownmemberinfo = currValue.split("\t");
					Long ownmemberHeartBeatSeq = Long.parseLong(ownmemberinfo[0]);
					String ownmemberstatus = ownmemberinfo[2];

					if (memberHeartBeatSeq > ownmemberHeartBeatSeq){
						ownmemberHeartBeatSeq = memberHeartBeatSeq;
						if (memberstatus.equals("V")) 
						{
							ownmemberstatus = "V"; // if a node wants to leave, it will mark itself as V in its membership list and gossip it, and other nodes immediately mark it as V
						}
						else{
							ownmemberstatus = "W"; // if the node was marked as failed, then if we get a gossip with a bigger sequence,it might still be working			 			 
						}
						newval =  Long.toString(ownmemberHeartBeatSeq)+"\t"+currtimestring+"\t"+ownmemberstatus;
						ownmemList.put(memberID, newval);

						// run_stabilization();	 not needed here 


						if (memberstatus.equals("V")) 
						{
							logger.info("[Leave] "+memberID+"\t"+newval);
						}

					}					
				}
			}
		}
	}

	public void update_selfentry()
	{
		Date currDate = new Date();
		SimpleDateFormat mlformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String currtimestring = mlformat.format(currDate.getTime());

		// update the local machine's heartbeat sequence in memlist
		String localmachinenewValues =  Long.toString(heartBeatSeq)+"\t"+currtimestring+"\t"+"W";
		ownmemList.put(machineID, localmachinenewValues);

		// run_stabilization();	// not needed here

	}

	public void check_timeout()
	{
		Date currDate = new Date();
		SimpleDateFormat mlformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Timestamp currTime = new Timestamp(currDate.getTime());
		String currtimestring = mlformat.format(currTime);

		Date timeStampDate = new Date();

		Set<String> keys = ownmemList.keySet();
		List<String> failedMembers = new ArrayList<String>();

		for (String memberID: keys){
			String currValue = ownmemList.get(memberID);
			String[] ownmemberinfo = currValue.split("\t");
			Long ownmemberHeartBeatSeq = Long.parseLong(ownmemberinfo[0]);
			try {
				timeStampDate = mlformat.parse(ownmemberinfo[1]);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			Timestamp memberLastUpdate = new Timestamp(timeStampDate.getTime());
			String ownmemberstatus = ownmemberinfo[2];

			String newval = null;

			if (((currTime.getTime() - memberLastUpdate.getTime()) >= TcleaninMS) && (ownmemberstatus.equals("F") || ownmemberstatus.equals("V"))){
				failedMembers.add(memberID);
				newval = Long.toString(ownmemberHeartBeatSeq)+"\t"+currtimestring+"\t"+"D";
				logger.info("[Delete] "+memberID+"\t"+newval);
			}


			if (((currTime.getTime() - memberLastUpdate.getTime()) >= TfailinMS) && ownmemberstatus.equals("W"))
			{
				newval = Long.toString(ownmemberHeartBeatSeq)+"\t"+currtimestring+"\t"+"F";
				ownmemList.put(memberID, newval);

				 // run_stabilization(); // not needed here 	 

				logger.info("[Fail] "+memberID+"\t"+newval);
			}
		}
		for (String memberID: failedMembers)
		{
			ownmemList.remove(memberID);	
		}

		if (!failedMembers.isEmpty())
		{
			run_stabilization();
		}

		failedMembers.clear();
	}

	public void receiver(){
		DatagramPacket receivePacket;
		DatagramSocket incomingsocket = null;
		byte[] receiveData = new byte[1024];
		receivePacket = new DatagramPacket(receiveData,receiveData.length);

		try{
			incomingsocket = new DatagramSocket(listeningport);	
			while(true){
				incomingsocket.receive(receivePacket);
				Random randomGenerator = new Random();
				double rand_num = randomGenerator.nextDouble();

				if (rand_num>=drop_rate)
				{	
					//					System.out.println("drop_rate"+drop_rate);
					msg_rec++;					
					byte_rec += receivePacket.getLength();
					String incomingEntry = new String(receivePacket.getData(),receivePacket.getOffset(),receivePacket.getLength());
					incomingMessageBuffer.add(incomingEntry);
				}
			} 
		}
		catch (IOException e) {
			e.printStackTrace();
		}	
	}

	public void election_rcv()
	{
		while(true)
		{
			try (   
					ServerSocket serversocket = new ServerSocket(listeningport);
					Socket clientsocket = serversocket.accept();
					BufferedReader in = new BufferedReader(new InputStreamReader(clientsocket.getInputStream()));
					){
				incomingElectionMsgBuffer.add(in.readLine());
			}
			catch(IOException e) {
				System.out.println("Exception in listening on port");
				System.out.println(e.getMessage());
				System.exit(-1);
			}
		}
	}

	public List<String> find_storage_nodes(String filename)
	{  
		List<String> tempmachineIP = new ArrayList<String>();
		List<String> machineIP = new ArrayList<String>();
		int hashedfile = stringtoHashedInteger(filename);
		String hashedfilestring = String.format("%03d",hashedfile); 
		List<String> hashedring = hashedRingMaker();
		List<String> modifiedring = hashedRingMaker();
		modifiedring.add(hashedfilestring);
		Collections.sort(modifiedring);

		String primarymachine = modifiedring.get((modifiedring.indexOf(hashedfilestring)+1)%modifiedring.size());

		tempmachineIP.add(primarymachine);
		int index = hashedring.indexOf(primarymachine);
		String secondmachine = hashedring.get((index+1)%hashedring.size());
		if (!tempmachineIP.contains(secondmachine))
		{
			tempmachineIP.add(secondmachine);
		}
		secondmachine = hashedring.get((index+2)%hashedring.size());
		if (!tempmachineIP.contains(secondmachine))
		{
			tempmachineIP.add(secondmachine);
		}
				System.out.println(hashedring);
				System.out.println(modifiedring);
				System.out.println(tempmachineIP);

		for (String key:tempmachineIP)
		{
			String[] keyarray = key.split("\t");
			machineIP.add(keyarray[1]);
		}
		return machineIP;
	}
	public void put_send(List<String> machineIP, int port, File tempfile, String sdfsfilename)
	{
		Socket socket;
		List<String> succ_transfermachine = new ArrayList<String>();
		for (String replicaMachine:machineIP)
		{
			try {
				socket = new Socket(replicaMachine,port);

				byte[] filedatabyte=new byte[8192];

				BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());
				BufferedInputStream in = new BufferedInputStream(new FileInputStream(tempfile));
				int count;
				while ((count = in.read(filedatabyte)) > 0) {
					out.write(filedatabyte, 0, count);
				}
				out.close();
				socket.close();
				in.close();		
				succ_transfermachine.add(replicaMachine);
			} 
			catch (IOException e) 
			{
				System.out.println("ERROR: PUTSEND: Put to "+replicaMachine+" failed");
			}
		}
		masterFileList.put(sdfsfilename,succ_transfermachine);
	}
	public void delete_send(List<String> filemachines, int fileopsport, byte[] deletecmd,String sdfsfilename)
	{
		Socket socket;
		int index = 0;
		String currentMachine = null;
		if (filemachines.size()>0)
		{
			while(index<filemachines.size())
			{  
				currentMachine = filemachines.get(index);
				index = index+1;
				try {
					socket = new Socket(currentMachine,fileopsport);
					BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());
					out.write(deletecmd, 0, deletecmd.length);
					out.close();
					socket.close();				
				} catch (IOException e) {
					System.out.println("ERROR: DELETESEND: Delete cmd to "+currentMachine+" failed");
				}
			}
		}
	}
	public void getfs_send(BufferedOutputStream toclientstream, String sdfsfilename, int fileopsport)
	{
		byte[] filedata = new byte[8192];
		int count;
		try 
		{
			System.out.println("Reading file "+sdfsfilename);
			BufferedInputStream fromFile = new BufferedInputStream(new FileInputStream(sdfsfilename.trim()));
			count = fromFile.read(filedata);
			System.out.println("From read "+count);
			while ((count = fromFile.read(filedata)) > 0) 
			{
				System.out.println("Wrote "+count);
				toclientstream.write(filedata, 0, count);
			}
			System.out.println("Sent file."+sdfsfilename);
			toclientstream.flush();
			fromFile.close();
			toclientstream.close();
		}
		catch (IOException e)
		{
			System.out.println("ERROR: GETFSSEND: "+sdfsfilename+" not found");
		}
	}

	public String process_arraylist(List<String> proclist)
	{
		String procstring = "";
		for(String key:proclist)
		{ 
			procstring=procstring+key+"\t";
		}
		return procstring;
	}

	public void run_stabilization()
	{
		Set<String> keys = null;
		Date date = new Date();
		Timestamp currtime = new Timestamp(date.getTime());
		Timestamp prevtime = currtime;
		//while(true)
		//{
			date = new Date();
			currtime = new Timestamp(date.getTime());
			//if ((selfAddress.equals(masterName)) && ((currtime.getTime()-prevtime.getTime()) >= 32*gossipTimeinMS))
			if ((selfAddress.equals(masterName)))	
			{
				prevtime=currtime;
				if (masterFileList.size() != 0)
				{
					keys = masterFileList.keySet();
					List<String> correctNodes = null;
					// for all the keys in the current list
					for (String filename:keys)
					{
						List<String> nodesmissingfiles = find_storage_nodes(filename); 
						List<String> correctnodes = find_storage_nodes(filename);
						List<String> currentFileNodes = masterFileList.get(filename);

						if (nodesmissingfiles !=null)
						{
							nodesmissingfiles.removeAll(masterFileList.get(filename));

							BufferedOutputStream toMachine = null;
							String newusercmd;
							byte[] filedata = new byte[8192]; 
							byte[] msgdata = new byte[8192];

							// send files to missing nodes
							// will it work in back to back failures ?	
							for (String currentMachine:nodesmissingfiles)
							{	
								for (String machineswithfile:currentFileNodes)
								{
									try {
										System.out.println("Stabilization: send "+filename+", "+machineswithfile+" to"+currentMachine);
										Socket currSocket = new Socket(currentMachine,fileopsport);
										toMachine = new BufferedOutputStream(currSocket.getOutputStream());
										newusercmd = "sget "+filename+" "+machineswithfile;
										msgdata=newusercmd.getBytes();
										filedata =  new byte[8192];
										filedata[0] = (byte) newusercmd.length();
										System.arraycopy(msgdata, 0, filedata, 1, msgdata.length);
										toMachine.write(filedata,0,filedata.length);
										toMachine.close();
										currSocket.close();
										System.out.println("Stabilization: End");
										
										//System.out.println("prevmachines initialization: "+prevmachines);
									
										List<String> prevmachines = masterFileList.get(filename); //masterFileList.get(filename);
										List<String> temp = new ArrayList<String>(prevmachines);   // masterFileList.get(filename); //new ArrayList<String>();
										
										temp.removeAll(find_storage_nodes(filename)); // nodes that should not have the file
										//System.out.println("temp, nodes that should not have the file: "+temp);
										prevmachines.add(currentMachine);
										//System.out.println("prevmachines after adding: "+prevmachines);
										prevmachines.removeAll(temp);
										//System.out.println("prevmachines final: "+prevmachines);
										masterFileList.put(filename, prevmachines);
										System.out.println("Stabilization: Update master file list");
										break;
									} catch (UnknownHostException e) {
										e.printStackTrace();
									} catch (IOException e) {
										//										e.printStackTrace();
										continue;
									}
								}
							}
							System.out.println("Stabilization: Nothing required.");
							System.out.println(getLineNumber());
						}

						List<String> nodestobedeleted = masterFileList.get(filename);

						if (nodestobedeleted.containsAll(correctnodes))
						{
							continue;
						}
						else
						{
							if (nodestobedeleted !=null)
								nodestobedeleted.removeAll(correctNodes);

							if (nodestobedeleted !=null)
							{
								//							nodestobedeleted.removeAll(correctNodes);
								BufferedOutputStream toMachine = null;
								String newusercmd;
								byte[] filedata = new byte[8192]; 
								byte[] msgdata = new byte[8192];

								if (nodestobedeleted.size() != 0)
								{
									// delete files from the nodes that should not have it 
									for (String currentMachine:nodestobedeleted)
									{	
										try {
											System.out.println("Stabilization: delete "+filename+" on "+currentMachine);
											Socket currSocket = new Socket(currentMachine,fileopsport);
											toMachine = new BufferedOutputStream(currSocket.getOutputStream());
											newusercmd = "deletefs "+filename;
											msgdata=newusercmd.getBytes();
											filedata =  new byte[8192];
											filedata[0] = (byte) newusercmd.length();
											System.arraycopy(msgdata, 0, filedata, 1, msgdata.length);
											toMachine.write(filedata,0,filedata.length);
											toMachine.close();
											currSocket.close();
											System.out.println("Stabilization: End");
											List<String> prevmachines = masterFileList.get(filename);
											prevmachines.remove(currentMachine);
											masterFileList.put(filename, prevmachines);
											System.out.println("Stabilization: Update master file list");
											break;
										} catch (UnknownHostException e) {
											e.printStackTrace();
										} catch (IOException e) {
											//										e.printStackTrace();
											continue;
										}
										System.out.println("Stabilization: Nothing required.");
										System.out.println(getLineNumber());
									}
								}
							}

						}
					}
				}
				
				List<String> ringnow = new ArrayList<String>();
				Set<String> keysnow = ownmemList.keySet();
				String[] machineIDArraynow = new String[3];
				machineIDArraynow = machineID.split(" ");
				ringnow.add(machineIDArraynow[0]);

				if (keysnow.size() > 0)
				{
					for (String key: keysnow)
					{
						machineIDArraynow = key.split(" ");
						ringnow.add(machineIDArraynow[0]);
					}
				}
				// only the alive machines in ringnow
				System.out.println("masterFileList at the end of stabilization:");
				// take out all the dead machines
				for (String fname:masterFileList.keySet())
				{
					List<String> currsys =  masterFileList.get(fname);
					for (String currmac:currsys)
					{
						if (!ringnow.contains(currmac))
							currsys.remove(currmac);
					}
					masterFileList.put(fname,currsys);
				}	
				System.out.println(masterFileList);
		}
	}


	public void start_file_operation_server() throws IOException
	{	
		byte[] bytedata = new byte[8192];
		String cmd;
		String cmdtype = null;
		int count, cmdlength=0;
		//		boolean byteRecord = false;
		ServerSocket serversocket =  new ServerSocket(fileopsport);
		Socket clientsocket,ftsocket = null;

		while(serversocket.isBound())
		{
			try {   
				clientsocket = serversocket.accept();
				File tempFile = null;
				BufferedInputStream fromClient = new BufferedInputStream(clientsocket.getInputStream());
				BufferedOutputStream toClient = new BufferedOutputStream(clientsocket.getOutputStream());
				BufferedOutputStream toMachine = null,toFile = null;
				BufferedInputStream  fromFile = null;
				String sdfsfilename = null;
				cmdlength=0;
				int totalbytes=0; 
				List<String> replicaMachines = new ArrayList<String>();

				while((count = fromClient.read(bytedata))>0)
				{
					totalbytes = totalbytes+count;
					// check if this is the first byte sequence of the incoming stream					
					if (cmdlength==0)
					{
						cmdlength = bytedata[0];
						cmd = new String(bytedata, 1, cmdlength);
						//cmd will be of the form "cmd filename"
						String[] cmdarray = cmd.split(" ");
						if(cmdarray[0].equals("put"))
						{
							//start collecting all the bytes
							cmdtype = "put";

							replicaMachines = find_storage_nodes(cmdarray[1].trim());
							System.out.println(replicaMachines);

							sdfsfilename = cmdarray[1].trim();
							String newcmd = "putfs "+cmdarray[1];
							byte[] filemsgarray=newcmd.getBytes();
							byte[] filedatabyte =  new byte[8192];
							filedatabyte[0] = (byte) newcmd.length();
							System.arraycopy(filemsgarray, 0, filedatabyte, 1, filemsgarray.length);

							tempFile = File.createTempFile("tmpfile",".tmp");
							toFile = new BufferedOutputStream(new FileOutputStream(tempFile));
							//write the command to the outputstream							
							toFile.write(filedatabyte,0,filedatabyte.length);	
							continue;
						}
						else if(cmdarray[0].equals("putfs"))
						{
							//write the file to a local file on the disk
							sdfsfilename = cmdarray[1].trim();
							File newFile = new File(sdfsfilename);
							toFile = new BufferedOutputStream(new FileOutputStream(newFile));
							cmdtype = "putfs";
							continue;
						}
						else if(cmdarray[0].equals("delete"))
						{
							sdfsfilename = cmdarray[1].trim();
							replicaMachines = masterFileList.get(sdfsfilename);
							String newcmd = "deletefs "+cmdarray[1];
							byte[] filemsgarray=newcmd.getBytes();
							byte[] filedatabyte =  new byte[8192];
							filedatabyte[0] = (byte) newcmd.length();
							System.arraycopy(filemsgarray, 0, filedatabyte, 1, filemsgarray.length);
							delete_send(replicaMachines, fileopsport, filedatabyte, sdfsfilename);
							masterFileList.remove(sdfsfilename);
							continue;	
						}
						else if(cmdarray[0].equals("deletefs"))
						{
							sdfsfilename = cmdarray[1].trim();
							File deletedfile = new File(sdfsfilename);
							deletedfile.delete();
							localFileList.remove(sdfsfilename);
						}

						else if(cmdarray[0].equals("sget"))
						{
							sdfsfilename = cmdarray[1].trim();
							String destIP = cmdarray[2];
							String sendermessage = "getfs "+sdfsfilename+" "+fileopsport;
							byte[] sendermsgbyte=sendermessage.getBytes();
							byte[] filedatabyte =  new byte[8192];
							filedatabyte[0] = (byte) sendermessage.length();
							System.arraycopy(sendermsgbyte, 0, filedatabyte, 1, sendermsgbyte.length);

							Socket sendermachine = new Socket(destIP,fileopsport);
							toMachine = new BufferedOutputStream(sendermachine.getOutputStream());
							//write the command to the outputstream							
							toMachine.write(filedatabyte,0,filedatabyte.length);
							toMachine.close();
						}

						else if(cmdarray[0].equals("get"))
						{
							System.out.println("Received: "+cmd);
							sdfsfilename = cmdarray[1].trim();
							replicaMachines = null;
							replicaMachines = masterFileList.get(sdfsfilename);
							String toclientmsg =null; 

							if (replicaMachines == null)
							{
								toclientmsg= "FileNotFound";
							}
							else
							{
								System.out.println("Replicas:"+process_arraylist(replicaMachines));
								toclientmsg= process_arraylist(replicaMachines);
							}

							byte[] toclientmsgbyte=toclientmsg.getBytes();
							byte[] filedatabyte =  new byte[8192];
							filedatabyte[0] = (byte) toclientmsg.length();
							System.arraycopy(toclientmsgbyte, 0, filedatabyte, 1, toclientmsgbyte.length);
							toClient.write(filedatabyte,0,filedatabyte.length);
							toClient.flush();
						}
						else if(cmdarray[0].equals("getfs"))
						{
							System.out.println("Received: "+cmd);
							sdfsfilename = cmdarray[1].trim();
							fromFile = new BufferedInputStream(new FileInputStream(sdfsfilename));
							int transferport = Integer.parseInt(cmdarray[2]);
							InetAddress ftmachine = clientsocket.getInetAddress();
							clientsocket.close();
							ftsocket = new Socket(ftmachine,transferport);
							//start collecting all the bytes
							cmdtype = "getfs";
							break;
						}			
						else if(cmdarray[0].equals("file"))
						{
							System.out.println("Received: "+cmd);
							sdfsfilename = cmdarray[1].trim();
							File newFile = new File(sdfsfilename);
							toFile = new BufferedOutputStream(new FileOutputStream(newFile));
							cmdtype = "putfs";
							continue;
						}
					}
					// out will be output stream to
					// in "put" an output stream to a temp file, with the modified command written
					// in putfs an output stream to sdfsfile with the file created already
					// in delete null
					// in deletefs null

					// this code will create the file for put and putfs 
					if (toFile !=null) toFile.write(bytedata, 0, count);

				}
				//all data has been read
				//close the buffer
				if (toFile!=null)
				{	
					toFile.flush();
					toFile.close();
				}

				// for put, finished completely reading the incoming file
				// do the putfs step and transfer it to machines that need it

				if (cmdtype != null)
				{
					if (cmdtype.equals("put"))
					{
						cmdtype = null;
						//send the file to the putsend function which relays the file to the relevant machines
						put_send(replicaMachines, fileopsport, tempFile,sdfsfilename);
						System.out.println("\nFile written to: "+masterFileList.toString());
						//file has been sent to the other machines, so delete temp file
						tempFile.delete();
						clientsocket.close();
					}
					else if (cmdtype.equals("getfs"))
					{
						cmdtype = null;
						//send the file to the putsend function which relays the file to the relevant machines
						BufferedOutputStream ftstream = new BufferedOutputStream(ftsocket.getOutputStream());
						String toreceivermsg = "file "+sdfsfilename;
						byte[] toclientmsgbyte=toreceivermsg.getBytes();
						byte[] filedatabyte =  new byte[8192];
						filedatabyte[0] = (byte) toreceivermsg.length();
						System.arraycopy(toclientmsgbyte, 0, filedatabyte, 1, toclientmsgbyte.length);
						ftstream.write(filedatabyte,0,filedatabyte.length);

						while ((count = fromFile.read(filedatabyte)) > 0) 
						{	
							ftstream.write(filedatabyte, 0, count);							
						}
						ftstream.close();
						System.out.println("File written to socket.");
						clientsocket.close();
						//file has been sent to the other machines, so delete temp file
						//						ftsocket.close();
					}
					else if (cmdtype.equals("putfs"))
					{
						cmdtype = null;
						if (!localFileList.contains(sdfsfilename))
							localFileList.add(sdfsfilename);
						System.out.println("Locallist: "+localFileList.toString());
					}
				}
				//				
			}
			catch(IOException e) {
				System.out.println("Exception in listening on port");
				System.out.println(e.getMessage());
				System.exit(-1);
			}
		}

		if (serversocket != null) serversocket.close();
	}

	@Override
	public void run() {
		Thread gossipRcvThread = new Thread(
				new Runnable() {
					public void run()
					{
						receiver();			
					}
				}
				);
		gossipRcvThread.setName("Gossip Receiving Thread");
		gossipRcvThread.start();

		Thread gossipSendThread = new Thread(
				new Runnable() {
					public void run()
					{
						try {
							sender();
						} catch (IOException e) {
							e.printStackTrace();
						}			
					}
				}
				);
		gossipSendThread.setName("Gossip Sending Thread");
		gossipSendThread.start();

		Thread electionSendThread = new Thread(
				new Runnable() {
					public void run()
					{
						try {
							election_send();
						} catch (IOException e) {
							e.printStackTrace();
						}			
					}
				}
				);
		electionSendThread.setName("Election Msg Sending Thread");
		electionSendThread.start();		

		Thread electionRcvThread = new Thread(
				new Runnable() {
					public void run()
					{
						election_rcv();
					}
				}
				);
		electionRcvThread.setName("Election Msg Receiving Thread");
		electionRcvThread.start();

		Thread fileOps = new Thread(
				new Runnable() {
					public void run()
					{
						try {
							start_file_operation_server();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				);
		fileOps.setName("File operations thread");
		fileOps.setDaemon(true);
		fileOps.start();

		/*
		Thread fileStab = new Thread(
				new Runnable() {
					public void run()
					{
						run_stabilization();
					}
				}
				);
		fileStab.setName("File stabilization thread");
		fileStab.setDaemon(true);
		fileStab.start();
		*/


		while(true){
			try{
				BufferedReader inputStream = new BufferedReader(new InputStreamReader(System.in));
				String userCmd;
				System.out.print("\nCMD:X(leave) | D(onetime display) | C/S(start/stop continuous display): ");
				while((userCmd=inputStream.readLine())!=null){
					String[] userCmdarray = userCmd.split(" ");
					if(userCmd.equals("X"))
					{
						currSysStatus = "V";
						System.out.print("System will exit in "+Double.toString(gossipTimeinMS/1000.0)+" s.\n");
						Thread.sleep(gossipTimeinMS);
						System.exit(0);
					}
					else if (userCmd.equals("D")){
						System.out.println(get_list(selfUpdateTime));
						System.out.print("\nCMD:X(leave) | D(onetime display) | C/S(start/stop continuous display): ");
					}
					else if (userCmd.equals("C")){
						displayFlag = true;
					}
					else if (userCmd.equals("S")){
						displayFlag = false;
						System.out.print("\nCMD:X(leave) | D(onetime display) | C/S(start/stop continuous display): ");
					}
					else if (userCmd.equals("store")){
						System.out.println(localFileList);
						System.out.print("\nCMD:X(leave) | D(onetime display) | C/S(start/stop continuous display): ");
					}
					else if (userCmdarray[0].equals("list"))
					{
						//System.out.print(masterName+" "+selfAddress);
						if (masterName.equals(selfAddress))
						{
							if (masterFileList.containsKey(userCmdarray[1]))
							{
								System.out.println(masterFileList.get(userCmdarray[1]));	
							}
							else 
							{
								System.out.print("file not in the system!");
							}	
						}
						else
						{
							System.out.print("Please ask the leader!: "+masterName);	
						}
						System.out.print("\nCMD:X(leave) | D(onetime display) | C/S(start/stop continuous display): ");
					}
					else
					{
						System.out.println("Please enter a valid cmd.");
						System.out.print("\nCMD:X(leave) | D(onetime display) | C/S(start/stop continuous display): ");
					}
				}

			}
			catch(IOException io){
				io.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}	
		}
	}

	public static int getLineNumber() {
    return Thread.currentThread().getStackTrace()[2].getLineNumber();
	}
}


