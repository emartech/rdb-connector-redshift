package com.emarsys.rdb.connector.redshift.utils

import com.emarsys.rdb.connector.redshift.RedshiftConnector.RedshiftConnectionConfig

object ConfigHelper {
  import com.typesafe.config.ConfigFactory

  val value = ConfigFactory.load().getString("dbconf.host")

  lazy val dbConnectorConfig = RedshiftConnectionConfig(
    host = ConfigFactory.load().getString("dbconf.host"),
    port = ConfigFactory.load().getInt("dbconf.port"),
    dbName = ConfigFactory.load().getString("dbconf.dbName"),
    dbUser = ConfigFactory.load().getString("dbconf.user"),
    dbPassword = ConfigFactory.load().getString("dbconf.password"),
    connectionParams = ConfigFactory.load().getString("dbconf.connectionParams")
  )
}
