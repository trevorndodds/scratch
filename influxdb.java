import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

public class DSBrokerStatsHook extends ServerHook {

	  private int timer = 30000;
	  private String db = "testDB";
	  private String dbServer = "127.0.0.1";
		  
		 public int getTimer()
		  {
		    return this.timer;
		  }
		  
		  public void setTimer(int timer)
		  {
		    this.timer = timer;
		  }	
		  
		 public String getDB()
		  {
		    return this.db;
		  }
		  
		  public void setDB(String db)
		  {
		    this.db = db;
		  }
		
		 public String getdbServer()
		  {
		    return this.dbServer;
		  }
		  
		  public void setdbServer(String dbServer)
		  {
		    this.dbServer = dbServer;
		  }
		  
		private Logger logger;	
	
	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void handleEvent(ServerEvent paramServerEvent) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void init() {
		// TODO Auto-generated method stub
	    this.logger = Logger.getLogger("BrokerStatsHook");
	    Logger localLogger = LogManager.getLogManager().getLogger("com.datasynapse");
	    this.logger.setParent(localLogger);
	    this.logger.setUseParentHandlers(true);
	    this.logger.info("initialized");
	    this.logger.info("Hook Properties: Enabled:" + getEnabled() + " Description:" + getDescription() + " Name:" + getName() + " Interval:" + getTimer() + " InfluxDB: " + getDB() + " InfluxDBServer: " + getdbServer());

	    startCollector(getTimer(), getEnabled(), getName(), getDB(), getdbServer() );
	}
	
	public void startCollector(int timer, boolean enabled, String name, String db, String dbServer )
	{
		
	    Thread t1 = new Thread(new Runnable() {
	    	private Logger logger;

			public void run()
	        {
			    this.logger = Logger.getLogger("BrokerStatsHook");
			    Logger localLogger = LogManager.getLogManager().getLogger("com.datasynapse");
			    this.logger.setParent(localLogger);
			    this.logger.setUseParentHandlers(true);
			    //this.logger.info("Sleeping for 60s to Complete Director Boot");
			    try {
					Thread.sleep(60000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			    //this.logger.info("Starting");
	    		while (enabled){
	    		    try {   			    

	    		    	int DriverCount = AdminManager.getDriverAdmin().getDriverCount();
		    		    int EngineCount = AdminManager.getEngineAdmin().getEngineCount();
		    		    int BusyEngineCount = AdminManager.getEngineAdmin().getBusyEngineCount();
		    		    int FinishedServiceCount = AdminManager.getServiceAdmin().getFinishedServiceCount();
		    		    int RunningServiceCount = AdminManager.getServiceAdmin().getRunningServiceCount();
		    		    int ServiceCount = AdminManager.getServiceAdmin().getServiceCount();
		    		    int InvocationCount = AdminManager.getServiceAdmin().getInvocationCount();
		    		    int PendingInvocationCount = AdminManager.getServiceAdmin().getPendingInvocationCount();
		    		    int RunningInvocationCount = AdminManager.getServiceAdmin().getRunningInvocationCount();
		    		    
		    		   // this.logger.info("Trying to Connect to influxDB");
		    		    InfluxDB influxDB = InfluxDBFactory.connect("http://" + dbServer + ":8086", "na", "");
		    		    String dbName = db;
		    		   // influxDB.createDatabase(dbName);
               //Batching
		    		    BatchPoints batchPoints = BatchPoints
		    	                .database(dbName)
		    	                .retentionPolicy("default")
		    	                .consistency(ConsistencyLevel.ALL)
		    	                .build();
		    		    
    		    
		    	//	    influxDB.enableBatch(100, 100, TimeUnit.MILLISECONDS);
		    	//	    this.logger.info("Get DS Values");
		    		    Point point1 = Point.measurement(name)
		                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
		                        .field("DriverCount", DriverCount)
		                        .field("EngineCount", EngineCount)
		                        .field("BusyEngineCount", BusyEngineCount)
		                        .field("FinishedServiceCount", FinishedServiceCount)
		                        .field("RunningServiceCount", RunningServiceCount)
		                        .field("ServiceCount", ServiceCount)
		                        .field("InvocationCount", InvocationCount)
		                        .field("PendingInvocationCount", PendingInvocationCount)
		                        .field("RunningInvocationCount", RunningInvocationCount)
		                        .useInteger(true)
		                        .build();
	    		    
		    		  //  influxDB.write(dbName, "default", point1);
		    		   
			    		   batchPoints.point(point1);
			    	  //   this.logger.info("Write Values");
			    		   influxDB.write(batchPoints);	
			    		//   this.logger.info("" + point1);
              
              			    				} catch (IOException e) {
			    					// TODO Auto-generated catch block
			    					this.logger.info("DirectorStatsHookError: " + e.getMessage());
			    				}
			    				
			    				client.close();
			    			} catch (UnknownHostException e) {
			    				// TODO Auto-generated catch block
			    				this.logger.info("DirectorStatsHookError: " + e.getMessage());
			    			}			    		   
			    		   
	    				//this.logger.info("DirectorStatsHook: sleep " + timer);
	    				Thread.sleep(timer);
	    			} catch (Exception e) {
	    				// TODO Auto-generated catch block
	    				this.logger.info("BrokerStatsHookError: " + e.getMessage());
	    			}

	    		}
	        }});  
	        t1.start();
	}	
}
