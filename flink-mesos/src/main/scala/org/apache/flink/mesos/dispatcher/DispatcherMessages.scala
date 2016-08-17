package org.apache.flink.mesos.dispatcher

import java.util.UUID

import org.apache.flink.runtime.messages.RequiresLeaderSessionID

/**
  */
object DispatcherMessages {

  /** Grants leadership to the receiver. The message contains the new leader session id.
    *
    * @param leaderSessionID
    */
  case class GrantLeadership(leaderSessionID: Option[UUID])

  /** Revokes leadership of the receiver.
    */
  case class RevokeLeadership()


  case class StartSession() extends RequiresLeaderSessionID

}
