package mesosphere.marathon.integration

import mesosphere.marathon.Protos.HealthCheckDefinition.Protocol
import mesosphere.marathon.api.v2.AppUpdate
import mesosphere.marathon.health.HealthCheck
import mesosphere.marathon.integration.setup._
import mesosphere.marathon.state.{ AppDefinition, Command, PathId }
import org.scalatest.{ BeforeAndAfter, GivenWhenThen, Matchers }
import org.slf4j.LoggerFactory
import play.api.libs.json.JsArray

import scala.concurrent.duration._

class AppDeployIntegrationTest
    extends IntegrationFunSuite
    with SingleMarathonIntegrationTest
    with Matchers
    with BeforeAndAfter
    with GivenWhenThen {

  private[this] val log = LoggerFactory.getLogger(getClass)

  //clean up state before running the test case
  before(cleanUp())

  test("create a simple app without health checks") {
    Given("a new app")
    val app = appProxy(testBasePath / "app", "v1", instances = 1, withHealth = false)

    When("The app is deployed")
    val result = marathon.createApp(app)

    Then("The app is created")
    result.code should be (201) //Created
    extractDeploymentIds(result) should have size 1
    waitForEvent("deployment_success")
    waitForTasks(app.id, 1) //make sure, the app has really started
  }

  test("create a simple app without health checks via secondary (proxying)") {
    if (!config.useExternalSetup) {
      Given("a new app")
      val app = appProxy(testBasePath / "app", "v1", instances = 1, withHealth = false)

      When("The app is deployed")
      val result = marathonProxy.createApp(app)

      Then("The app is created")
      result.code should be (201) //Created
      extractDeploymentIds(result) should have size 1
      waitForEvent("deployment_success")
      waitForTasks(app.id, 1) //make sure, the app has really started
    }
  }

  test("create a simple app with http health checks") {
    Given("a new app")
    val app = appProxy(testBasePath / "http-app", "v1", instances = 1, withHealth = false).
      copy(healthChecks = Set(healthCheck))
    val check = appProxyCheck(app.id, "v1", true)

    When("The app is deployed")
    val result = marathon.createApp(app)

    Then("The app is created")
    result.code should be (201) //Created
    extractDeploymentIds(result) should have size 1
    waitForEvent("deployment_success")
    check.pingSince(5.seconds) should be (true) //make sure, the app has really started
  }

  test("create a simple app with tcp health checks") {
    Given("a new app")
    val app = appProxy(testBasePath / "tcp-app", "v1", instances = 1, withHealth = false).
      copy(healthChecks = Set(healthCheck.copy(protocol = Protocol.TCP)))

    When("The app is deployed")
    val result = marathon.createApp(app)

    Then("The app is created")
    result.code should be (201) //Created
    extractDeploymentIds(result) should have size 1
    waitForEvent("deployment_success")
  }

  test("create a simple app with command health checks") {
    Given("a new app")
    val app = appProxy(testBasePath / "command-app", "v1", instances = 1, withHealth = false).
      copy(healthChecks = Set(healthCheck.copy(protocol = Protocol.COMMAND, command = Some(Command("true")))))

    When("The app is deployed")
    val result = marathon.createApp(app)

    Then("The app is created")
    result.code should be (201) //Created
    extractDeploymentIds(result) should have size 1
    waitForEvent("deployment_success")
  }

  test("list running apps and tasks") {
    Given("a new app is deployed")
    val appId = testBasePath / "app"
    val app = appProxy(appId, "v1", instances = 2, withHealth = false)
    marathon.createApp(app).code should be (201) //Created

    When("the deployment has finished")
    waitForEvent("deployment_success")

    Then("the list of running app tasks can be fetched")
    val apps = marathon.listAppsInBaseGroup
    apps.code should be(200)
    apps.value should have size 1

    val tasks = marathon.tasks(appId)
    tasks.code should be(200)
    tasks.value should have size 2
  }

  test("create an app that fails to deploy") {
    Given("a new app that is not healthy")
    val appId = testBasePath / "failing"
    val check = appProxyCheck(appId, "v1", state = false)
    val app = appProxy(appId, "v1", instances = 1, withHealth = false).
      copy(healthChecks = Set(HealthCheck(gracePeriod = 20.second, interval = 1.second, maxConsecutiveFailures = 10)))

    When("The app is deployed")
    val create = marathon.createApp(app)

    Then("The deployment can not be finished")
    create.code should be (201) //Created
    extractDeploymentIds(create) should have size 1
    intercept[AssertionError] {
      waitForEvent("deployment_success")
    }

    When("The app is deleted")
    val delete = marathon.deleteApp(appId, force = true)
    delete.code should be (200)
    waitForChange(delete)
    marathon.listAppsInBaseGroup.value should have size 0
  }

  test("update an app") {
    Given("a new app")
    val appId = testBasePath / "app"
    val v1 = appProxy(appId, "v1", instances = 1, withHealth = true)
    marathon.createApp(v1).code should be (201)
    waitForEvent("deployment_success")
    val before = marathon.tasks(appId)

    When("The app is updated")
    val check = appProxyCheck(appId, "v2", state = true)
    val update = marathon.updateApp(v1.id, AppUpdate(cmd = appProxy(appId, "v2", 1).cmd))

    Then("The app gets updated")
    update.code should be (200)
    waitForEvent("deployment_success")
    waitForTasks(appId, before.value.size)
    check.pingSince(5.seconds) should be (true) //make sure, the new version is alive
  }

  test("scale an app up and down") {
    Given("a new app")
    val app = appProxy(testBasePath / "app", "v1", instances = 1, withHealth = false)
    marathon.createApp(app)
    waitForEvent("deployment_success")

    When("The app get an update to be scaled up")
    val saleUp = marathon.updateApp(app.id, AppUpdate(instances = Some(2)))

    Then("The app is scaled up")
    saleUp.code should be (200) //OK
    waitForEvent("deployment_success")
    waitForTasks(app.id, 2)

    When("The app get an update to be scaled down")
    val scaleDown = marathon.updateApp(app.id, AppUpdate(instances = Some(1)))

    Then("The app is scaled down")
    scaleDown.code should be (200) //OK
    waitForEvent("deployment_success")
    waitForTasks(app.id, 1)
  }

  test("restart an app") {
    Given("a new app")
    val appId = testBasePath / "app"
    val v1 = appProxy(appId, "v1", instances = 1, withHealth = false)
    marathon.createApp(v1).code should be (201)
    waitForEvent("deployment_success")
    val before = marathon.tasks(appId)

    When("The app is restarted")
    val restart = marathon.restartApp(v1.id)

    Then("All instances of the app get restarted")
    restart.code should be (200)
    waitForChange(restart)
    val after = marathon.tasks(appId)
    waitForTasks(appId, before.value.size)
    before.value.toSet should not be after.value.toSet
  }

  test("list app versions") {
    Given("a new app")
    val v1 = appProxy(testBasePath / "app", "v1", instances = 1, withHealth = false)
    marathon.createApp(v1).code should be (201)
    waitForEvent("deployment_success")

    When("The app is restarted")
    val list = marathon.listAppVersions(v1.id)

    Then("All instances of the app get restarted")
    list.code should be (200)
    list.value.versions should have size 1
  }

  test("kill a task of an App") {
    Given("a new app")
    val app = appProxy(testBasePath / "app", "v1", instances = 1, withHealth = false)
    marathon.createApp(app).code should be (201)
    waitForEvent("deployment_success")
    val taskId = marathon.tasks(app.id).value.head.id

    When("a task of an app is killed")
    marathon.killTask(app.id, taskId)
    waitForEventWith("status_update_event", _.info("taskStatus") == "TASK_KILLED")

    Then("All instances of the app get restarted")
    waitForTasks(app.id, 1)
    marathon.tasks(app.id).value.head should not be taskId
  }

  test("kill a task of an App with scaling") {
    Given("a new app")
    val app = appProxy(testBasePath / "app", "v1", instances = 2, withHealth = false)
    marathon.createApp(app).code should be (201)
    waitForEvent("deployment_success")
    val taskId = marathon.tasks(app.id).value.head.id

    When("a task of an app is killed and scaled")
    marathon.killTask(app.id, taskId, scale = true)
    waitForEventWith("status_update_event", _.info("taskStatus") == "TASK_KILLED")

    Then("All instances of the app get restarted")
    waitForTasks(app.id, 1)
    marathon.app(app.id).value.instances should be (1)
  }

  test("kill all tasks of an App") {
    Given("a new app with multiple tasks")
    val app = appProxy(testBasePath / "app", "v1", instances = 2, withHealth = false)
    marathon.createApp(app).code should be (201)
    waitForEvent("deployment_success")

    When("all task of an app are killed")
    marathon.killAllTasks(app.id)
    waitForEventWith("status_update_event", _.info("taskStatus") == "TASK_KILLED")
    waitForEventWith("status_update_event", _.info("taskStatus") == "TASK_KILLED")

    Then("All instances of the app get restarted")
    waitForTasks(app.id, 2)
  }

  ignore("kill all tasks of an App with scaling") {
    Given("a new app with multiple tasks")
    val app = appProxy(testBasePath / "tokill", "v1", instances = 2, withHealth = false)
    marathon.createApp(app).code should be (201)
    waitForEvent("deployment_success")

    When("all task of an app are killed")
    val result = marathon.killAllTasks(app.id, scale = true)
    result.value.tasks should have size 2
    waitForEventWith("status_update_event", _.info("taskStatus") == "TASK_KILLED")
    waitForEventWith("status_update_event", _.info("taskStatus") == "TASK_KILLED")

    Then("All instances of the app get restarted")
    waitForTasks(app.id, 0)
    //TODO: this is a bug in marathon: it will update the instance count to "something"
    marathon.app(app.id).value.instances should be (0)
  }

  test("delete an application") {
    Given("a new app with one task")
    val app = appProxy(testBasePath / "app", "v1", instances = 1, withHealth = false)
    marathon.createApp(app).code should be (201)
    waitForEvent("deployment_success")

    When("the app is deleted")
    val delete = marathon.deleteApp(app.id)
    delete.code should be (200)
    waitForChange(delete)

    Then("All instances of the app get restarted")
    marathon.listAppsInBaseGroup.value should have size 0
  }

  test("create and deploy an app with two tasks") {
    Given("a new app")
    log.info("new app")
    val appIdPath: PathId = testBasePath / "/test/app"
    val appId: String = appIdPath.toString
    val app = appProxy(appIdPath, "v1", instances = 2, withHealth = false)

    When("the app gets posted")
    log.info("new app")
    val createdApp: RestResult[AppDefinition] = marathon.createApp(app)

    Then("the app is created and a success event arrives eventually")
    log.info("new app")
    createdApp.code should be(201) // created

    Then("we get various events until deployment success")
    val deploymentIds: Seq[String] = extractDeploymentIds(createdApp)
    deploymentIds.length should be(1)
    val deploymentId = deploymentIds.head

    log.info("waiting for deployment success")
    val events: Map[String, Seq[CallbackEvent]] = waitForEvents(
      "api_post_event", "group_change_success", "deployment_info",
      "status_update_event", "status_update_event",
      "deployment_success")(30.seconds)

    val Seq(apiPostEvent) = events("api_post_event")
    apiPostEvent.info("appDefinition").asInstanceOf[Map[String, Any]]("id").asInstanceOf[String] should
      be(appId)

    val Seq(groupChangeSuccess) = events("group_change_success")
    groupChangeSuccess.info("groupId").asInstanceOf[String] should be(appIdPath.parent.toString)

    val Seq(taskUpdate1, taskUpdate2) = events("status_update_event")
    taskUpdate1.info("appId").asInstanceOf[String] should be(appId)
    taskUpdate2.info("appId").asInstanceOf[String] should be(appId)

    val Seq(deploymentSuccess) = events("deployment_success")
    deploymentSuccess.info("id") should be(deploymentId)

    Then("after that deployments should be empty")
    val event: RestResult[List[Deployment]] = marathon.listDeploymentsForBaseGroup()
    event.value should be('empty)

    Then("Both tasks respond to http requests")
    def pingTask(taskInfo: CallbackEvent): RestResult[String] = {
      val host: String = taskInfo.info("host").asInstanceOf[String]
      val port: Int = taskInfo.info("ports").asInstanceOf[Seq[Int]].head
      appMock.ping(host, port)
    }

    pingTask(taskUpdate1).entityString should be(s"Pong $appId\n")
    pingTask(taskUpdate2).entityString should be(s"Pong $appId\n")
  }

  def healthCheck = HealthCheck(gracePeriod = 20.second, interval = 1.second, maxConsecutiveFailures = 10)

  def extractDeploymentIds(app: RestResult[AppDefinition]): Seq[String] = {
    for (deployment <- (app.entityJson \ "deployments").as[JsArray].value)
      yield (deployment \ "id").as[String]
  }
}
