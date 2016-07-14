package org.apache.flink.mesos.scheduler;

import org.apache.flink.mesos.scheduler.messages.Disconnected;
import org.apache.flink.mesos.scheduler.messages.Error;
import org.apache.flink.mesos.scheduler.messages.Error;
import org.apache.flink.mesos.scheduler.messages.OfferRescinded;
import org.apache.flink.mesos.scheduler.messages.ReRegistered;
import org.apache.flink.mesos.scheduler.messages.Registered;
import org.apache.flink.mesos.scheduler.messages.ResourceOffers;
import org.apache.flink.mesos.scheduler.messages.SlaveLost;
import org.apache.flink.mesos.scheduler.messages.StatusUpdate;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.List;

/**
 * This class reacts to callbacks from the Mesos scheduler driver.
 *
 * In order to preserve actor concurrency safety, this class simply sends
 * corresponding messages to the Mesos resource master actor.
 *
 * See https://mesos.apache.org/api/latest/java/org/apache/mesos/Scheduler.html
 */
public class SchedulerProxy implements Scheduler {

	/** The actor to which we report the callbacks */
	private ActorGateway mesosActor;

	public SchedulerProxy(ActorGateway mesosActor) {
		this.mesosActor = mesosActor;
	}

	@Override
	public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
		mesosActor.tell(new Registered(frameworkId, masterInfo));
	}

	@Override
	public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
		mesosActor.tell(new ReRegistered(masterInfo));
	}

	@Override
	public void disconnected(SchedulerDriver driver) {
		mesosActor.tell(new Disconnected());
	}


	@Override
	public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
		mesosActor.tell(new ResourceOffers(offers));
	}

	@Override
	public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
		mesosActor.tell(new OfferRescinded(offerId));
	}

	@Override
	public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
		mesosActor.tell(new StatusUpdate(status));
	}

	@Override
	public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
		throw new UnsupportedOperationException("frameworkMessage is unexpected");
	}

	@Override
	public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
		mesosActor.tell(new SlaveLost(slaveId));
	}

	@Override
	public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
		throw new UnsupportedOperationException("executorLost is unexpected");
	}

	@Override
	public void error(SchedulerDriver driver, String message) {
		mesosActor.tell(new Error(message));
	}

	/**
	 * Leaders may change. The current gateway can be adjusted here.
	 * @param gateway The current gateway to the leading actor instance.
	 */
	public void setCurrentLeaderGateway(ActorGateway gateway) {
		this.mesosActor = gateway;
	}
}
