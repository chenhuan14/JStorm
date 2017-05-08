package wangmen.myscheduler;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.client.WorkerAssignment;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {

		public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
			TopologyBuilder builder = new TopologyBuilder();
			
			
			builder.setSpout("sentence-spout", new RandomSentenceSpout(),1);
			
			builder.setBolt("split-bolt", new SpiltBolt(),4).shuffleGrouping("sentence-spout");
			
			builder.setBolt("count-bolt", new CountBolt(),1).fieldsGrouping("split-bolt", new Fields("word"));
			
			Config conf = new Config();
			
			List<WorkerAssignment> userDefines = new ArrayList<>();
			
			WorkerAssignment worker = new WorkerAssignment();
			worker.addComponent("split-bolt", 2);
			worker.setHostName("node18-18.pdl.net");
			userDefines.add(worker);
			
			
			
			WorkerAssignment worker1 = new WorkerAssignment();
			worker.addComponent("split-bolt", 2);
			worker.setHostName("node18-17.pdl.net");
			userDefines.add(worker1);
			
			
			ConfigExtension.setUserDefineAssignment(conf, userDefines);
			
			
			
			conf.setDebug(true);
	        
			if (args != null && args.length > 0) {
	        	conf.setNumWorkers(3);
	        	StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	        }
			else{
				conf.setMaxTaskParallelism(3);
				LocalCluster cluster = new LocalCluster();  
				cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());  
				Thread.sleep(Integer.MAX_VALUE);  
				cluster.shutdown();  
			}
		}
}
