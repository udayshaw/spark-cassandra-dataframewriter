
class CassandraSinkForeach() extends ForeachWriter[org.apache.spark.sql.Row] {
  // This class implements the interface ForeachWriter, which has methods that get called
  // whenever there is a sequence of rows generated as output

  var cassandraDriver: CassandraDriver = null;
  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
  //  println(s"Open connection")
    true
  }

  def process(record: org.apache.spark.sql.Row) = {
   // println(s"Process new $record")
    if (cassandraDriver == null) {
      cassandraDriver = new CassandraDriver();
    }
     try {   
      cassandraDriver.connector.withSessionDo(session => session.execute(s"""
       insert into ${cassandraDriver.namespace}.${cassandraDriver.foreachTableSink} (field0,field1,field2,field3,field4,field5,field6,field7,field8,field9,field10,field11,field12)
       values(${record(0)}, '${record(1)}', '${record(2)}', '${record(3)}', '${record(4)}', '${record(5)}', '${record(6)}', '${record(7)}', '${record(8)}', '${record(9)}', '${record(10)}', '${record(11)}', '${record(12)}')"""))
    }
     catch {
      case unknown: Exception => {
        println(s"Error in: $record")
        //println(s"Unknown exception: $unknown")
        //Failure(unknown)
      }
    }
   }

  def close(errorOrNull: Throwable): Unit = {
    // close the connection
   // println(s"Close connection")
     val con="Close connection" 
  }
}

//val sink = df2.writeStream.queryName("UserData_with_IP").outputMode("update").foreach(new CassandraSinkForeach()).start()
//sink.awaitTermination()

