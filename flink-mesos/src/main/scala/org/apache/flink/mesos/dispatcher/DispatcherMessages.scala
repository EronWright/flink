package org.apache.flink.mesos.dispatcher

import java.util.UUID

import org.apache.flink.mesos.dispatcher.types.{SessionID, SessionParameters, SessionStatus}
import org.apache.flink.runtime.messages.RequiresLeaderSessionID

/**
  * Dispatcher actor messages.
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

  /**
    * Start a new session.
    */
  case class StartSession(params: SessionParameters) extends RequiresLeaderSessionID

  /**
    * Stop a session.
    */
  case class StopSession(sessionID: SessionID) extends RequiresLeaderSessionID

  /**
    * A status update message.
    *
    * @param sessionStatus the status.
    */
  case class SessionStatusUpdate(sessionStatus: SessionStatus)

}
