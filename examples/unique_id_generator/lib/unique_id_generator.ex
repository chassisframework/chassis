defmodule UniqueIdGenerator do
  alias Chassis.RandomPartitionRoutingStrategy

  @type id :: integer()
  @type cluster_name :: Chassis.cluster_name()

  def start(cluster_name, bootstrap_nodes) do
    Chassis.start_cluster(cluster_name, {UniqueIdGenerator.VNode, []}, {UniqueIdGenerator.State, []}, bootstrap_nodes)
  end

  def start_local(cluster_name, opts \\ []) do
    Chassis.start_local_cluster(cluster_name, {UniqueIdGenerator.VNode, []}, {UniqueIdGenerator.State, []}, opts)
  end

  def get(cluster_name) do
    Chassis.call(cluster_name, :get, routing_strategy: RandomPartitionRoutingStrategy)
  end

  def partition_state(cluster_name, partition_id) do
    Chassis.call(cluster_name, :state, partition_id: partition_id)
  end
end
