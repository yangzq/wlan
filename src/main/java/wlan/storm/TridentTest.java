package wlan.storm;

import backtype.storm.generated.*;
import org.apache.thrift7.TApplicationException;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.protocol.TProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: yangzq2
 * Date: 13-3-15
 * Time: 下午6:16
 */
public class TridentTest {
   public static void main(String[] args){
       TTransport transport = null;
       String address = "10.1.253.93"; // nimbus部署服务器
       int port = 6627; // nimbus监听端口
       int clientTimeout = 5000;
       try {
           transport = new TFramedTransport(new TSocket(address, port,
                   clientTimeout));
           TProtocol protocol = new TBinaryProtocol(transport);
           Nimbus.Client client = new Nimbus.Client(protocol);
           transport.open();
           List<TopologySummary> topologySummaries = client.getClusterInfo().get_topologies();
           //storm监控页面的信息都在这个list里
           String component = "updateTimeBolt";
           Set<String> hosts = new HashSet<String>();
           if (topologySummaries.size() > 0) {
               TopologySummary topologySummary = topologySummaries.get(0);
               TopologyInfo topologyInfo = client.getTopologyInfo(topologySummary.get_id());
               for (ExecutorSummary executorSummary : topologyInfo.get_executors()) {
                   System.out.println(String.format("%s:%s", executorSummary.get_component_id(), executorSummary.get_stats()));
                   // 所有的状态信息都可以从get_stats()中获取
                   if (component.equals(executorSummary.get_component_id())) {
                       hosts.add(executorSummary.get_host());
                   }
               }
               Iterator iterator = hosts.iterator();
               while (iterator.hasNext()){
                   System.out.println(iterator.next());
               }
           }
       } catch (TApplicationException e1) {
           e1.printStackTrace();
       } catch (TException e2) {
           e2.printStackTrace();
       } catch (NotAliveException e3) {
           e3.printStackTrace();
       } finally {
           if (transport != null) {
               transport.close();
           }
       }
   }

}
