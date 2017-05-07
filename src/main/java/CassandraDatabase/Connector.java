package CassandraDatabase;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class Connector {
	private Cluster cluster;
	private Session session;
	
	public Cluster getCluster() {
		return cluster;
	}
	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}
	public Session getSession() {
		return session;
	}
	public void setSession(Session session) {
		this.session = session;
	}
	public Connector() {
		this.cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042)
				.build();
		Metadata metadata = cluster.getMetadata();
		System.out.println("Connected to cluster: " + metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Data center %s, host %s, rack %s \n", host.getDatacenter(), host.getAddress(),
					host.getRack());
		}
		this.session = this.connect();
	}
	public Session connect(){
		return this.cluster.connect();
	}
	public void close(){
		this.cluster.close();
	}
}
