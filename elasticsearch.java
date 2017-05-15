import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.logging.log4j.Log4jESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

//ElasticSearch
			    		   
	DateFormat df = new SimpleDateFormat("yyyy.MM.dd");
	Date today = Calendar.getInstance().getTime();
	String indexDate = df.format(today);

	Settings settings = Settings.settingsBuilder()
		.put("client.transport.ignore_cluster_name", true).build();
	try {

		Log4jESLoggerFactory.getLogger("").setLevel("WARN");

			Client client = TransportClient.builder().settings(settings).build()
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300))
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 19300));

			try {
				@SuppressWarnings("unused")
				IndexResponse response = client.prepareIndex("trev_test-"+indexDate, "metrics")
					.setSource(jsonBuilder()
						    .startObject()
							.field("@timestamp", new Date())
							.field("Manager", name)
							.field("DriverCount", DriverCount)
							.field("EngineCount", EngineCount)
							.field("BusyEngineCount", BusyEngineCount)
							.field("FinishedServiceCount", FinishedServiceCount)
							.field("RunningServiceCount", RunningServiceCount)
							.field("ServiceCount", ServiceCount)
							.field("InvocationCount", InvocationCount)
							.field("PendingInvocationCount", PendingInvocationCount)
							.field("RunningInvocationCount", RunningInvocationCount)
						    .endObject()
						  )
					.get();

//			 System.out.println(response);
//			 System.out.println(response.isCreated());
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
