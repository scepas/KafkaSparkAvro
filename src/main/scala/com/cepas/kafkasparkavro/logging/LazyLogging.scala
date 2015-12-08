package com.cepas.kafkasparkavro.logging

import org.slf4j.{LoggerFactory, Logger}

/**
  * Created by scepas on 8/12/15.
  */
trait LazyLogging {
    protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}
