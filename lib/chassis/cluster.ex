defmodule Chassis.Cluster do
  @moduledoc false

  alias Chassis.Cluster.Forward
  alias Chassis.Cluster.Membership
  alias Chassis.Cluster.RaftGroup
  alias Chassis.Cluster.Router
  alias Chassis.Cluster.VNode
  alias Chassis.Cluster.Partition
  alias Chassis.ClusterState
  alias Chassis.ClusterState.Config
  alias Chassis.Raft
  alias Chassis.Reply
  alias Chassis.Request

  require Logger

  def start_cluster(
        cluster_name,
        v_node_app,
        state_apps,
        strategy,
        strategy_args,
        starter_pid
      ) do
    config = %Config{
      v_node_app: v_node_app,
      state_apps: state_apps,
      cluster_name: cluster_name,
      strategy: strategy
    }

    layout = PartitionedClusterLayout.new(strategy, strategy_args)

    :ok = Membership.start_group(config, layout, starter_pid)

    ready_msg = Membership.cluster_ready_msg(cluster_name)

    receive do
      ^ready_msg ->
        :ok
    after
      60_000 ->
        raise "cluster #{cluster_name} taking too long to start..."
    end
  end

  defdelegate destroy_cluster(cluster_name), to: Membership
  defdelegate cluster_names(), to: RaftGroup, as: :known_cluster_names

  def call(request, cluster_state) do
    request
    |> leader_v_node(cluster_state)
    |> GenServer.call(request)
    |> case do
      %Reply{} = reply ->
        reply

      %Forward{to_partition_id: to_partition_id, to_nodes: [leader | _rest]} ->
        v_node_name =
          cluster_state
          |> ClusterState.cluster_name()
          |> VNode.name(to_partition_id)

        {v_node_name, leader} |> GenServer.call(request)

      error ->
        error
    end
  end

  def cast(request, cluster_name) do
    cluster_name
    |> Router.get_state()
    |> Router.route(request)
    |> leader_v_node(cluster_name)
    |> GenServer.cast(request)
  end

  def get_cluster_state(cluster_name, opts) do
    from_cache = Keyword.get(opts, :cached, false)

    if from_cache do
      Router.get_state(cluster_name)
    else
      Membership.get_state!(cluster_name)
    end
  end

  def partition_ids(%ClusterState{} = cluster_state) do
    ClusterState.partition_ids(cluster_state)
  end

  def stage_node_join(cluster_name, node, args) do
    Membership.stage_node_join(cluster_name, node, args)
  end

  def commit_stage(cluster_name) do
    Membership.commit_stage(cluster_name)
  end

  def subscribe(cluster_name) do
    Membership.subscribe(cluster_name)
  end

  defp leader_v_node(%Request{partition_id: partition_id}, cluster_state) do
    v_node_name =
      cluster_state
      |> ClusterState.cluster_name()
      |> VNode.name(partition_id)

    {:ok, leader} = Partition.leader(cluster_state, partition_id)

    {v_node_name, leader}
  end
end
