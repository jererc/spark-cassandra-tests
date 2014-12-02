Create a local_settings.json file to override the default conf:


        {
            "cassandraHost" : "",
            "cassandraRpcPort" : "9160"
            "cassandraKeyspace" : "test_keyspace"
            "cassandraInputTable" : "test_table_input"
            "cassandraOutputTable" : "test_table_output"
            "sparkMasterHost" : ""
            "sparkMasterPort" : "7077"
        }


./sbt assembly run
