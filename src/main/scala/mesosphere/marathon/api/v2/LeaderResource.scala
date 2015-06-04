package mesosphere.marathon.api.v2

import javax.ws.rs.core.{ MediaType, Response }
import javax.ws.rs.{ GET, DELETE, Path, Produces }

import com.google.inject.Inject
import mesosphere.chaos.http.HttpConf
import mesosphere.marathon.api.{ LeaderInfo, RestResource }
import mesosphere.marathon.{ MarathonSchedulerService, MarathonConf }

@Path("v2/leader")
class LeaderResource @Inject() (
  leaderInfo: LeaderInfo,
  schedulerService: MarathonSchedulerService,
  val config: MarathonConf with HttpConf)
    extends RestResource {

  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  def index(): Response = {
    leaderInfo.currentLeaderHostPort() match {
      case None => notFound("There is no leader")
      case Some(leader) =>
        ok(Map("leader" -> leader))
    }
  }

  @DELETE
  @Produces(Array(MediaType.APPLICATION_JSON))
  def delete(): Response = {
    leaderInfo.elected match {
      case false => notFound("There is no leader")
      case true =>
        schedulerService.abdicateLeadership()
        ok(Map("message" -> "Leadership abdicted"))
    }
  }
}
