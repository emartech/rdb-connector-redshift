package com.emarsys.rdb.connector.redshift

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.redshift.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.RawSelectItSpec
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContextExecutor

class RedshiftRawSelectItSpec extends TestKit(ActorSystem()) with RawSelectItSpec with SelectDbInitHelper with WordSpecLike  with Matchers with BeforeAndAfterAll {

  implicit val materializer: Materializer = ActorMaterializer()

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def afterAll(): Unit = {
    system.terminate()
    cleanUpDb()
    connector.close()
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  override val simpleSelect = s"""SELECT * FROM "$aTableName";"""
  override val badSimpleSelect = s"""SELECT * ForM "$aTableName""""
  override val simpleSelectNoSemicolon = s"""SELECT * FROM "$aTableName""""

}