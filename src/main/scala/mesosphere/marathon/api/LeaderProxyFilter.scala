package mesosphere.marathon.api

import java.util

import mesosphere.chaos.http.HttpConf
import org.apache.http.HttpStatus
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import com.google.inject.Inject
import java.net.{ ConnectException, HttpURLConnection, URL }
import java.io.{ OutputStream, InputStream }
import javax.inject.Named
import javax.servlet._
import javax.servlet.http.{ HttpServletResponse, HttpServletRequest }
import mesosphere.marathon.{ LeaderProxyConf, ModuleNames }
import org.apache.log4j.Logger

import scala.util.Try
import scala.util.control.NonFatal

/**
  * Info about leadership.
  */
trait LeaderInfo {
  /** Query whether we are elected as the current leader. This should be cheap. */
  def elected: Boolean

  /**
    * Query the host/port of the current leader if any. This might involve network round trips
    * and should be called sparingly.
    *
    * @return `Some(`<em>host</em>:</em>port</em>`)`, e.g. my.local.domain:8080 or `None` if
    *        the leader is currently unknown
    */
  def currentLeaderHostPort(): Option[String]
}

/**
  * Servlet filter that proxies requests to the leader if we are not the leader.
  */
class LeaderProxyFilter @Inject() (httpConf: HttpConf,
                                   leaderInfo: LeaderInfo,
                                   @Named(ModuleNames.NAMED_HOST_PORT) myHostPort: String,
                                   forwarder: RequestForwarder)
    extends Filter {

  import LeaderProxyFilter._

  def init(filterConfig: FilterConfig): Unit = {}

  private[this] val scheme = if (httpConf.disableHttp()) "https" else "http"

  private[this] def buildUrl(leaderData: String, request: HttpServletRequest): URL = {
    if (request.getQueryString != null) {
      new URL(s"$scheme://$leaderData${request.getRequestURI}?${request.getQueryString}")
    }
    else {
      new URL(s"$scheme://$leaderData${request.getRequestURI}")
    }
  }

  @tailrec
  final def doFilter(rawRequest: ServletRequest,
                     rawResponse: ServletResponse,
                     chain: FilterChain) {

    def waitForConsistentLeadership(response: HttpServletResponse): Boolean = {
      var retries = 10

      do {
        val weAreLeader = leaderInfo.elected
        val currentLeaderData = leaderInfo.currentLeaderHostPort()

        if (weAreLeader || currentLeaderData.exists(_ != myHostPort)) {
          log.info("Leadership info is consistent again!")
          return true
        }

        if (retries >= 0) {
          log.info(s"Waiting for consistent leadership state. Are we leader?: $weAreLeader, leader: $currentLeaderData")
          sleep()
        }
        else {
          log.error(
            s"inconsistent leadership state, refusing request for ourselves at $myHostPort. " +
              s"Are we leader?: $weAreLeader, leader: $currentLeaderData")
        }

        retries -= 1
      } while (retries >= 0)

      false
    }

    (rawRequest, rawResponse) match {
      case (request: HttpServletRequest, response: HttpServletResponse) =>
        lazy val leaderDataOpt = leaderInfo.currentLeaderHostPort()

        if (leaderInfo.elected) {
          chain.doFilter(request, response)
        }
        else if (leaderDataOpt.forall(_ == myHostPort)) { // either not leader or ourselves
          log.info(
            s"Do not proxy to myself. Waiting for consistent leadership state. " +
              s"Are we leader?: false, leader: $leaderDataOpt")
          if (waitForConsistentLeadership(response)) {
            doFilter(rawRequest, rawResponse, chain)
          }
          else {
            response.sendError(HttpStatus.SC_SERVICE_UNAVAILABLE, ERROR_STATUS_NO_CURRENT_LEADER)
          }
        }
        else {
          try {
            val url: URL = buildUrl(leaderDataOpt.get, request)
            forwarder.forward(url, request, response)
          }
          catch {
            case NonFatal(e) =>
              throw new RuntimeException("while proxying", e)
          }
        }
      case _ =>
        throw new IllegalArgumentException(s"expected http request/response but got $rawRequest/$rawResponse")
    }
  }

  protected def sleep(): Unit = {
    Thread.sleep(250)
  }

  def destroy() {
    //NO-OP
  }
}

object LeaderProxyFilter {
  private val log = Logger.getLogger(getClass.getName)
  val ERROR_STATUS_NO_CURRENT_LEADER: String = "Could not determine the current leader"
}

/**
  * Forwards a HttpServletRequest to an URL.
  */
trait RequestForwarder {
  def forward(url: URL, request: HttpServletRequest, response: HttpServletResponse): Unit
}

class JavaUrlConnectionRequestForwarder @Inject() (
  leaderProxyConf: LeaderProxyConf, @Named(ModuleNames.NAMED_HOST_PORT) myHostPort: String)
    extends RequestForwarder {

  import JavaUrlConnectionRequestForwarder._

  private[this] val viaValue: String = s"1.1 $myHostPort"

  override def forward(url: URL, request: HttpServletRequest, response: HttpServletResponse): Unit = {

    val viaOpt = Option(request.getHeaders(HEADER_VIA)).map(_.asScala.toVector)
    log.debug("viaOpt {}", viaOpt)
    if (viaOpt.exists(_.contains(viaValue))) {
      log.error("Prevent proxy cycle, rejecting request")
      response.sendError(HttpStatus.SC_BAD_GATEWAY, ERROR_STATUS_LOOP)
      return
    }

    val method = request.getMethod

    log.info(s"Proxying request to $method $url from $myHostPort")

    val leaderConnection: HttpURLConnection = url.openConnection().asInstanceOf[HttpURLConnection]

    leaderConnection.setConnectTimeout(leaderProxyConf.leaderProxyConnectionTimeout())
    leaderConnection.setReadTimeout(leaderProxyConf.leaderProxyReadTimeout())

    // getHeaderNames() and getHeaders() are known to return null, see:
    //http://docs.oracle.com/javaee/6/api/javax/servlet/http/HttpServletRequest.html#getHeaders(java.lang.String)
    val names = Option(request.getHeaderNames).map(_.asScala).getOrElse(Nil)
    for {
      name <- names if !name.equalsIgnoreCase("host") && !name.equalsIgnoreCase("connection")
      headerValues <- Option(request.getHeaders(name))
      headerValue <- headerValues.asScala
    } {
      log.debug(s"addRequestProperty $name: $headerValue")
      leaderConnection.addRequestProperty(name, headerValue)
    }

    leaderConnection.addRequestProperty(HEADER_VIA, viaValue)

    leaderConnection.setRequestMethod(method)

    log.debug("request properties {}", leaderConnection.getRequestProperties.asScala)

    method match {
      case "GET" | "HEAD" | "DELETE" =>
        leaderConnection.setDoOutput(false)
      case _ =>
        leaderConnection.setDoOutput(true)

        val proxyOutputStream = leaderConnection.getOutputStream
        try {
          copy(request.getInputStream, proxyOutputStream)
        }
        finally {
          Try(proxyOutputStream.close())
        }
    }

    try {
      val status = leaderConnection.getResponseCode
      response.setStatus(status)

      val fields = leaderConnection.getHeaderFields
      // getHeaderNames() and getHeaders() are known to return null
      if (fields != null) {
        for ((name, values) <- fields.asScala) {
          if (name != null && values != null) {
            for (value <- values.asScala) {
              response.addHeader(name, value)
            }
          }
        }
      }
      copy(leaderConnection.getInputStream, response.getOutputStream)
    }
    catch {
      case connException: ConnectException =>
        response.sendError(HttpStatus.SC_BAD_GATEWAY, ERROR_STATUS_CONNECTION_REFUSED)
      case NonFatal(e) =>
        copy(leaderConnection.getErrorStream, response.getOutputStream)
    }
    finally {
      Try(response.getOutputStream.close())
      Try(leaderConnection.getInputStream.close())
      Try(leaderConnection.getErrorStream.close())
    }
  }

  private[this] def copy(inputOrNull: InputStream, outputOrNull: OutputStream): Unit = {
    for {
      input <- Option(inputOrNull)
      output <- Option(outputOrNull)
    } {
      val bytes = new Array[Byte](1024)
      Iterator
        .continually(input.read(bytes))
        .takeWhile(_ != -1)
        .foreach(read => output.write(bytes, 0, read))
    }
  }
}

object JavaUrlConnectionRequestForwarder {
  private val log = LoggerFactory.getLogger(getClass)

  /** Header for proxy loop detection. Simply "Via" is ignored by the URL connection.*/
  val HEADER_VIA: String = "X-Marathon-Via"
  val ERROR_STATUS_LOOP: String = "Detected proxying loop."
  val ERROR_STATUS_CONNECTION_REFUSED: String = "Connection to leader refused."

}