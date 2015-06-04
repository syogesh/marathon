
package mesosphere.marathon.integration.setup

import com.google.common.util.concurrent.Service.{ State, Listener }
import org.apache.commons.io.FileUtils

import scala.sys.ShutdownHookThread
import scala.sys.process._
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import com.google.inject.Guice
import org.rogach.scallop.ScallopConf
import com.google.common.util.concurrent.{ AbstractIdleService, Service }
import mesosphere.chaos.http.{ HttpService, HttpConf, HttpModule }
import mesosphere.chaos.metrics.MetricsModule
import java.io.{ Closeable, File }
import java.util.concurrent.{ Executor, TimeUnit }
import org.apache.log4j.Logger
import scala.concurrent.{ duration, Await, Promise }
import scala.concurrent.duration._

/**
  * Book Keeper for processes and services.
  * During integration tests, several services and processes have to be launched.
  * The ProcessKeeper knows about them and can handle their lifecycle.
  */
object ProcessKeeper {

  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] var processes = List.empty[Process]
  private[this] var services = List.empty[Service]

  private[this] val ENV_MESOS_WORK_DIR: String = "MESOS_WORK_DIR"

  def startHttpService(port: Int, assetPath: String) = {
    startService {
      log.info(s"Start Http Service on port $port")
      val conf = new ScallopConf(Array("--http_port", port.toString, "--assets_path", assetPath)) with HttpConf
      conf.afterInit()
      val injector = Guice.createInjector(new MetricsModule, new HttpModule(conf), new IntegrationTestModule)
      injector.getInstance(classOf[HttpService])
    }
  }

  def startZooKeeper(port: Int, workDir: String) {
    val args = "org.apache.zookeeper.server.ZooKeeperServerMain" :: port.toString :: workDir :: Nil
    val workDirFile = new File(workDir)
    FileUtils.deleteDirectory(workDirFile)
    FileUtils.forceMkdir(workDirFile)
    startJavaProcess("zookeeper", args, new File("."), sys.env, _.contains("binding to port"))
  }

  def startMesosLocal(): Process = {
    val mesosWorkDirForMesos: String = "/tmp/marathon-itest-mesos"
    val mesosWorkDirFile: File = new File(mesosWorkDirForMesos)
    FileUtils.deleteDirectory(mesosWorkDirFile)
    FileUtils.forceMkdir(mesosWorkDirFile)
    startProcess(
      "mesos",
      Process(Seq("mesos-local", "--ip=127.0.0.1"), cwd = None, ENV_MESOS_WORK_DIR -> mesosWorkDirForMesos),
      upWhen = _.toLowerCase.contains("registered with master"))
  }

  def startMarathon(cwd: File, env: Map[String, String], arguments: List[String],
                    mainClass: String = "mesosphere.marathon.Main",
                    startupLine: String = "Started SelectChannelConnector"): Process = {

    val argsWithMain = mainClass :: arguments

    val mesosWorkDir: String = "/tmp/marathon-itest-marathon"
    val mesosWorkDirFile: File = new File(mesosWorkDir)
    FileUtils.deleteDirectory(mesosWorkDirFile)
    FileUtils.forceMkdir(mesosWorkDirFile)

    startJavaProcess(
      "marathon", argsWithMain, cwd,
      env + (ENV_MESOS_WORK_DIR -> mesosWorkDir),
      upWhen = _.contains(startupLine))
  }

  def startJavaProcess(name: String, arguments: List[String],
                       cwd: File = new File("."), env: Map[String, String] = Map.empty, upWhen: String => Boolean): Process = {
    log.info(s"Start java process $name with args: $arguments")
    val javaExecutable = sys.props.get("java.home").fold("java")(_ + "/bin/java")
    val classPath = sys.props.getOrElse("java.class.path", "target/classes")
    val builder = Process(javaExecutable :: "-classpath" :: classPath :: arguments, cwd, env.toList: _*)
    val process = startProcess(name, builder, upWhen)
    log.info(s"Java process $name up and running!")
    process
  }

  def startProcess(name: String, processBuilder: ProcessBuilder, upWhen: String => Boolean, timeout: Duration = 30.seconds): Process = {
    val up = Promise[Boolean]()
    val logger = new ProcessLogger {
      def checkUp(out: String) = {
        log.info(s"$name: $out")
        if (!up.isCompleted && upWhen(out)) up.trySuccess(true)
      }
      override def buffer[T](f: => T): T = f
      override def out(s: => String) = checkUp(s)
      override def err(s: => String) = checkUp(s)
    }
    val process = processBuilder.run(logger)
    Try(Await.result(up.future, timeout)) match {
      case Success(_) => processes = process :: processes
      case Failure(_) =>
        process.destroy()
        throw new IllegalStateException(s"Process does not came up within time bounds ($timeout). Give up. $processBuilder")
    }
    process
  }

  def onStopServices(block: => Unit): Unit = {
    services ::= new AbstractIdleService {
      override def shutDown(): Unit = {
        block
      }

      override def startUp(): Unit = {}
    }
  }

  def stopOSProcesses(grep: String): Unit = {
    val PIDRE = """\s*(\d+)\s.*""".r
    val processes = ("ps -x" #| s"grep $grep").!!.split("\n").map { case PIDRE(pid) => pid }
    processes.foreach(p => s"kill -9 $p".!)
  }

  def stopAllProcesses(): Unit = {

    def waitForProcessesToFinish(): Unit = {
      processes.foreach(p => Try(p.destroy()))

      // Unfortunately, there seem to be race conditions in Process.exitValue.
      // Thus this ugly workaround.
      val waitForExitInThread = new Thread() {
        override def run(): Unit = {
          processes.foreach(_.exitValue())
        }
      }
      waitForExitInThread.start()
      try {
        waitForExitInThread.join(1000)
      }
      finally {
        waitForExitInThread.interrupt()
      }
    }

    try waitForProcessesToFinish()
    catch {
      case NonFatal(e) =>
        log.error("while waiting for processes to finish", e)
        try waitForProcessesToFinish()
        catch {
          case NonFatal(e) =>
            log.error("giving up waiting for processes to finish", e)
        }
    }
    processes = Nil
  }

  def startService(service: Service): Unit = {
    services ::= service
    service.startAsync().awaitRunning()
  }

  def stopAllServices(): Unit = {
    services.foreach(_.stopAsync())
    services.par.foreach(_.awaitTerminated(5, TimeUnit.SECONDS))
    services = Nil
  }

  def shutdown(): Unit = {
    stopAllProcesses()
    stopAllServices()
  }

  val shutDownHook: ShutdownHookThread = sys.addShutdownHook {
    shutdown()
  }

  def main(args: Array[String]) {
    //startMarathon(new File("."), Map("MESOS_NATIVE_LIBRARY" -> "/usr/local/lib/libmesos.dylib"), List("--master", "local", "--event_subscriber", "http_callback"))
    startZooKeeper(2183, "/tmp/foo")
    Thread.sleep(10000)
    stopAllProcesses()
    //startHttpService(11211, ".")
  }
}

