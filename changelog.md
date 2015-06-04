## Changes from 0.8.2 to 0.9.0

#### Recommended Mesos version is 0.22.1

We tested this release against Mesos version 0.22.1. Thus this is the recommended
Mesos version for this release.

#### Disk resource limits are passed to Mesos

If you specify a non-zero disk resource limit, this limit is now passed to Mesos
on task launch.

If you rely on disk limits, you also need to configure Mesos appropriately. This includes configuring the correct
isolator and enabling disk quotas enforcement with `--enforce_container_disk_quota`.

#### New format for the `http_endpoints` parameter

We changed the format of the `http_endpoints` parameters from a
space-separated to a comma-separated list of endpoints, in order to be
more consistent with the documentation and with the format used in other
parameters.

WARNING: If you use the `http_endpoints` parameter with multiple space
separated URLs, you will need to migrate to the comma-separated format.

#### Improved proxying to current leader

One of the Marathon instances is always elected as a leader and is the only instance processing your requests.
For convenience, Marathon has long proxied all requests to non-leaders to the current leader so that
you do not have to lookup the current leader yourself or are annoyed by redirects.

This proxying has now been improved and gained additional configuration parameters:

* `--leader_proxy_connection_timeout` (Optional. Default: 5000):
    Maximum time, in milliseconds, for connecting to the
    current Marathon leader from this Marathon instance.
* `--leader_proxy_read_timeout` (Optional. Default: 10000):
    Maximum time, in milliseconds, for reading from the
    current Marathon leader.

These bugs are now obsolete:

- #1540 A marathon instance should never proxy to itself
- #1541 Proxying Marathon requests should use timeouts

## Changes from 0.8.1 to 0.8.2

#### New health check option `ignoreHttp1xx`

When set to true, the health check for the given app will ignore HTTP
response codes 100 to 199, in contrast to considering it as unhealthy. With this unbounded task startup times can be handled: the tasks are neither
healthy nor unhealthy as long as e.g. "100 - continue" is returned.

#### HTTPS support for health checks

Health checks now work with HTTPS.

#### Faster (configurable) task distribution

In order to speed up task launching and use the resource offers Marathon receives from Mesos more efficiently,
we added a new offer matching algorithm, that tries to start as many tasks as possible per task offer cycle.
The maximum numbers of tasks to start are configurable with the follwing startup parameters:

`--max_tasks_per_offer` (default 1): The maximum number of tasks to start on a single offer per cycle

`--max_tasks_per_offer_cycle` (default 1000): The maximum number of tasks to start in total per cycle

#### Security settings configurable through env variables

Security settings are now configurable through the following environment variables:

`$MESOSPHERE_HTTP_CREDENTIALS` for HTTP authentication

`$MESOSPHERE_KEYSTORE_PATH` + `$MESOSPHERE_KEYSTORE_PASS` for SSL settings

## Changes from 0.8.0 to 0.8.1

#### New option `dryRun` on endpoint `PUT /v2/groups/{id}`

When sending a group definition to this endpoint with `dryRun=true`,
it will return the deployment steps Marathon would execute to deploy
this group.

#### New endpoint POST `/v2/tasks/delete`

Takes a JSON object containing an array of task ids and kills them.
If `?scale=true` the tasks will not be restarted and the `instances`
field of the affected apps will be adjusted.

#### POST `/v2/apps` rejects existing ids

If an app with an already existing id is posted to this endpoint,
it will now be rejected

#### PUT `/v2/apps/{id}` always returns deployment info

In versions <= 0.8.0 it used to return the complete app definition
if the resource didn't exist before. To be consistent in the response,
it has been changed to always return the deployment info instead. However
it still return a `201 - Created` if the resource didn't exist.

#### GET `/v2/queue` includes delay

In 0.8.0 the queueing behavior has changed and the output of this endpoint
did not contain the delay field anymore. In 0.8.1 we re-added this field.

