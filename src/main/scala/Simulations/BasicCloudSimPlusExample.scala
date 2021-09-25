package Simulations

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyRoundRobin
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple
import org.cloudbus.cloudsim.cloudlets.{Cloudlet, CloudletSimple}
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.datacenters.DatacenterSimple
import org.cloudbus.cloudsim.hosts.HostSimple
import org.cloudbus.cloudsim.provisioners.{PeProvisionerSimple, ResourceProvisionerSimple}
import org.cloudbus.cloudsim.resources.{Pe, PeSimple}
import org.cloudbus.cloudsim.schedulers.vm.{VmScheduler, VmSchedulerTimeShared}
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic
import org.cloudbus.cloudsim.vms.{Vm, VmSimple}
import org.cloudsimplus.builders.tables.CloudletsTableBuilder
import org.cloudsimplus.listeners.CloudletVmEventInfo
import org.cloudbus.cloudsim.network.topologies.BriteNetworkTopology
import org.cloudbus.cloudsim.schedulers.cloudlet.{CloudletSchedulerCompletelyFair, CloudletSchedulerSpaceShared, CloudletSchedulerTimeShared}

import collection.JavaConverters.*

class BasicCloudSimPlusExample

object BasicCloudSimPlusExample:
  val config = ObtainConfigReference("cloudSimulator") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  val logger = CreateLogger(classOf[BasicCloudSimPlusExample])
  val cloudsim = new CloudSim();
  val broker0 = new DatacenterBrokerSimple(cloudsim);

  //making 4 hosts per vm
  /*
  //cancel cloudlet halfway function
  private def cancelCloudletIfHalfExecuted(e: CloudletVmEventInfo): Unit = {
    val cloudlet = e.getCloudlet
    if (cloudlet.getFinishedLengthSoFar >= config.getLong("cloudSimulator.cloudlet.size") / 2) {
      System.out.printf("%n# %.2f: Intentionally cancelling %s execution after it has executed half of its length.%n", e.getTime, cloudlet)
      cloudlet.getVm.getCloudletScheduler.cloudletCancel(cloudlet)
    }
  }
  val utilizationModel = new UtilizationModelDynamic(config.getDouble("cloudSimulator.utilizationRatio"));

  //clone a single cloudlet function
  private def cloneCloudlet(source: Cloudlet): Cloudlet = {
    val clone = new CloudletSimple(source.getLength, source.getNumberOfPes)
    clone.setId(source.getId * 10)
    clone.setUtilizationModel(utilizationModel)
    clone.setLength(config.getLong("cloudSimulator.cloudlet.size"))
    clone.setNumberOfPes(config.getLong("cloudSimulator.cloudlet.PEs"))
    clone
  }
  //clone list of cloudlets
  private def cloneCloudlets(sourceVm: Vm) = {
    val sourceVmCloudlets = sourceVm.getCloudletScheduler.getCloudletList.asScala
    //val clonedCloudlets = List(Cloudlet)
    val clonedCloudlets = sourceVmCloudlets.map(x=>cloneCloudlet(x))
    clonedCloudlets
  }*/
  //function to create a datacenter to implement round robin
  private def createDataCenter() ={
    val hostPes = List.fill(4)(new PeSimple(config.getLong("cloudSimulator.host.mipsCapacity").toDouble,new PeProvisionerSimple()))
    val hosts = List.fill(1)(new HostSimple(config.getLong("cloudSimulator.host.RAMInMBs"),
      config.getLong("cloudSimulator.host.StorageInMBs"),
      config.getLong("cloudSimulator.host.BandwidthInMBps"),
      hostPes.asJava)
    .setVmScheduler(new VmSchedulerTimeShared())
    .setRamProvisioner(new ResourceProvisionerSimple())
    .setBwProvisioner(new ResourceProvisionerSimple()))

    val dataCenter = new DatacenterSimple(cloudsim, hosts.asJava, new VmAllocationPolicyRoundRobin)
    dataCenter
  }
  //function to create 30 vms
  private def createtVm() = {

    List.fill(2)(
      new VmSimple(config.getLong("cloudSimulator.vm.mipsCapacity").toDouble, config.getInt("cloudSimulator.vm.PEs"))
        .setRam(config.getLong("cloudSimulator.vm.RAMInMBs"))
        .setBw(config.getLong("cloudSimulator.vm.BandwidthInMBps"))
        .setSize(config.getLong("cloudSimulator.vm.StorageInMBs"))
        //.setCloudletScheduler(new CloudletSchedulerCompletelyFair)
        //.setCloudScheduler(new CloudletSchedulerTimeShared)
        .setCloudletScheduler(new CloudletSchedulerSpaceShared)
    )


  }
  //function creating cloudlets
  private def createCloudlets() = {
    val utilizationModel = new UtilizationModelDynamic(0.5)
    List(
    new CloudletSimple(config.getLong("cloudSimulator.cloudlet.size"), config.getInt("cloudSimulator.cloudlet.PEs"), utilizationModel)
      .setLength(1000),
    new CloudletSimple(config.getLong("cloudSimulator.cloudlet.size"), config.getInt("cloudSimulator.cloudlet.PEs"), utilizationModel)
      .setLength(8000),
    new CloudletSimple(config.getLong("cloudSimulator.cloudlet.size"), config.getInt("cloudSimulator.cloudlet.PEs"), utilizationModel)
      .setLength(10000),
    new CloudletSimple(config.getLong("cloudSimulator.cloudlet.size"), config.getInt("cloudSimulator.cloudlet.PEs"), utilizationModel)
      .setLength(12000)
    )
  }


  def Start() =
    logger.info(s"Created one processing element: ")
    //created two hosts

    logger.info(s"Created two host")
    val dc0 = createDataCenter()
    val dc1 = createDataCenter()
    dc0.setSchedulingInterval(1)
    dc1.setSchedulingInterval(1)

    dc0.setVmAllocationPolicy(new VmAllocationPolicyRoundRobin);

    logger.info(s"Created virtual machine:")

    val utilizationModel = new UtilizationModelDynamic(config.getDouble("cloudSimulator.utilizationRatio"));

    logger.info(s"Created a list of cloudlets")
    val networkTopology= new BriteNetworkTopology
    cloudsim.setNetworkTopology(networkTopology)
    networkTopology.addLink(dc0, broker0, config.getLong("cloudSimulator.NETWORK_BW").toDouble, config.getLong("cloudSimulator.NETWORK_LATENCY").toDouble)
    networkTopology.addLink(dc1, broker0, config.getLong("cloudSimulator.NETWORK_BW").toDouble, config.getLong("cloudSimulator.NETWORK_LATENCY").toDouble)

    //Maps CloudSim entities to BRITE entities
    //Datacenter0 will correspond to BRITE node 0

    networkTopology.mapNode(dc0, 0)

    networkTopology.mapNode(dc1, 2)

    networkTopology.mapNode(broker0, 3)
    val vmList = createtVm()
    broker0.submitVmList(vmList.asJava);
    val cloudletList = createCloudlets()
    logger.info("cancellation attempt halfway");
    //cloudletList(0).addOnUpdateProcessingListener(this.cancelCloudletIfHalfExecuted)

    broker0.bindCloudletToVm(cloudletList(0), vmList(0))
    broker0.bindCloudletToVm(cloudletList(0), vmList(1))

    broker0.submitCloudletList(cloudletList.asJava);


    logger.info("Starting cloud simulation...")
    cloudsim.start();

    new CloudletsTableBuilder(broker0.getCloudletFinishedList()).build();
