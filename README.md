# multi_connection_pool
this project finished a multi connection pool for thrift,http or other protocals , which can rebalance and keep alive
you can initial the multi pool from server urls list just like this:   

go get "github.com/minicaptain/multi_connection_pool/util"  

then import the package:

import "github.com/minicaptain/multi_connection_pool/util"  


/****************************************************************/

testPool := &util.MultiPool{  
		
		SingleMaxActiveCount: 10,  
		
		SingleMaxIdleCount:  2,  
		
		IdleTimeOut:  time.Second * 5,  
		
		SingleFactory:func(server string, time.Second, 1) util.Factory {
   },  
   
		Servers:             []string{srv1, srv2, srv3},  
		
	}  
	
	testPool.InitFromServerList()  
	
/****************************************************************/
  params:  
  
  SingeMaxActiveCount: the single pool max active connection count, the multi pool total active connection count 
  should be len(testPool.Servers) * testPool.SingleMaxActiveCount.  
  
  SingleMaxIdleCount: the idle connection count, which control the max idle connection count of single pool.  
  
  IdleTimeOut: the connection in idle list should be close after this duration.  
  
  SingleFactory: the factory produces conn.  
  
  Servers: the server url list.  
  
  Also you can create the multi connection pool from zk node, which keep balance through zk virtual node:  
  
 /****************************************************************/  
 
 testConnPool = &util.MultiPool{
		ZkServers:            []{127.0.0.1:2181, 127.0.0.2:2181},  
		
		ZkNode:               /test/servers,  
		
		SingleMaxActiveCount: 10,  
		
		SingleMaxIdleCount:   2,  
		
		IdleTimeOut:          time.Second * 5,  
		
		SingleFactory:        func(server string, time.Second, 1) util.Factory {
   },  
   
		ZkSessionTimeout:     time.Second,  
		
	}  
	
	testConnPool.InitFromZkChildrenNode()  
	
  /****************************************************************/
  
  int the program finished a simple thirft server and thrift client pool.  
  

