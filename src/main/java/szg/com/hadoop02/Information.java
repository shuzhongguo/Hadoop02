package szg.com.hadoop02;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.Inet4Address;
import java.net.Socket;
import java.net.UnknownHostException;

public class Information {
	
	public static String getHost(){
		try {
			String hostname = Inet4Address.getLocalHost().getHostName();
			return hostname;
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return "have Exceptions";
		}
	}
	
	public static String getPID(){
		RuntimeMXBean rmxb = ManagementFactory.getRuntimeMXBean();
		String pid = rmxb.getName().split("@")[0];
		return pid;
	}
	public static String getTID(){
		Thread current  = Thread.currentThread();
		String threadName = current.getName();
		long tid = current.getId();
		return "\tTID:"+tid+"\tThreadName:"+threadName;
	}
	public static String getMethod(){
		String className = new Throwable().getStackTrace()[1].getClassName();
		String methodName = new Throwable().getStackTrace()[1].getMethodName();
		return "\tClassName:"+className+"\tMethodName:"+methodName;
	}

	public static String getInfo() {
		return "HostName:"+getHost()+ "\tPID:"+getPID()+getTID()+getMethod();
	}
	
	public static void outInfo(Socket socket,Object obj){
		try {
			socket.getOutputStream().write((getInfo()+"\t"+obj.toString()+"\r\n").getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		};
	}
}
