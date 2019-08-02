defmodule Chassis.Cluster.Membership do
  @moduledoc false

  require Logger

  alias Chassis.ClusterState
  alias Chassis.Cluster.Membership.Machine, as: MembershipMachine
  alias Chassis.Cluster.RaftGroup
  alias PartitionedClusterLayout.Node

  @group_id :membership

  def start_group(config, layout, starter_pid) do
    node_names =
      layout
      |> PartitionedClusterLayout.nodes()
      |> Enum.map(fn %Node{name: name} -> name end)

    # bug in :ra fails to restart raft clusters if their config contains a pid, so we term_to_bin it until that's fixed
    cluster_state = %ClusterState{
      # init_args: init_args,
      config: config,
      layout: layout,
      starter_pid: :erlang.term_to_binary(starter_pid)
    }

    :ok = RaftGroup.start_group(@group_id, cluster_state, MembershipMachine, nil, node_names)
    :ok = command(cluster_state, {:accept_initial_state, cluster_state})
  end

  #
  # TODO: parallelize?
  #
  def start_known_membership_replicas do
    # Enum.each(known_cluster_names(), fn cluster_name ->
    #   Logger.info("Starting membership replica", cluster_name: cluster_name)

    #   :ok =
    #     cluster_name
    #     |> consensus_group_id()
    #     |> Raft.restart_server()
    # end)
  end

  def destroy_cluster(cluster_name) do
    command(cluster_name, :destroy_cluster)
  end

  def destroy_membership_group(cluster_state) do
    nodes = ClusterState.node_names(cluster_state)

    RaftGroup.destroy_group(@group_id, cluster_state, nodes)
  end

  def add_node(cluster_state, node) do
    nodes = ClusterState.node_names(cluster_state)

    RaftGroup.add_member(@group_id, cluster_state, node, MembershipMachine, nil, nodes)
  end

  def get_state(cluster_name, nodes) do
    query(cluster_name, nodes, {MembershipMachine, :state_query_function, []})
  end

  def get_state!(cluster_name, nodes) do
    {:ok, state} = query(cluster_name, nodes, {MembershipMachine, :state_query_function, []})

    state
  end

  #
  # TODO: fetch strategy from leader and locally run validate_add_node_args/2
  #
  def stage_node_join(cluster_state, node, args) do
    command(cluster_state, {:stage, {:join, node, args}})
  end

  def commit_stage(cluster_state) do
    command(cluster_state, {:stage, :commit})
  end

  def command(cluster_state, command) do
    nodes = ClusterState.node_names(cluster_state)

    {:ok, response} = RaftGroup.command(@group_id, cluster_state, command, nodes)

    response
  end

  def query(%ClusterState{} = cluster_state, query) do
    nodes = ClusterState.node_names(cluster_state)

    RaftGroup.query(@group_id, cluster_state, query, nodes)
  end

  def query(cluster_name, nodes, query) do
    RaftGroup.query(@group_id, cluster_name, query, nodes)
  end

  def subscribe(cluster_name) do
    command(cluster_name, {:subscribe, self()})
  end

  def cluster_ready_msg(cluster_name) do
    {:"$Chassis_cluster_ready$", cluster_name}
  end

  def partition_ready(cluster_state, partition_id) do
    command(cluster_state, {:partition_ready, partition_id})
  end
end
