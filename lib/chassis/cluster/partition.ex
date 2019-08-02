defmodule Chassis.Cluster.Partition do
  @moduledoc false

  require Logger

  import Chassis.Util, only: [parallel_each: 2]

  alias Chassis.ClusterState
  alias Chassis.Cluster.Partition.Machine, as: PartitionMachine
  alias Chassis.Cluster.RaftGroup
  alias PartitionedClusterLayout.VNode

  def create_initial_partitions(%ClusterState{} = cluster_state) do
    cluster_state
    |> ClusterState.v_nodes_by_partition_id()
    |> parallel_each(fn {partition_id, v_nodes} ->
      create_partition(cluster_state, partition_id, v_nodes)
    end)
  end

  def create_partition(cluster_state, partition_id, v_nodes, transition_ranges \\ nil, confirmation_message \\ nil) do
    args = %{cluster_state: cluster_state, partition_id: partition_id, transition_ranges: transition_ranges, confirmation_message: confirmation_message}

    node_names = Enum.map(v_nodes, fn %VNode{node_name: node_name} -> node_name end)

    :ok =
      partition_id
      |> raft_group_id()
      |> RaftGroup.start_group(cluster_state, PartitionMachine, args, node_names)
  end

  def state_app_ready(cluster_state, partition_id, name) do
    command(
      cluster_state,
      partition_id,
      {:state_app_ready, name},
      ClusterState.node_names(cluster_state, partition_id)
    )
  end

  # def destroy_partitions(cluster_state) do
  #   cluster_state
  #   |> ClusterState.v_nodes_by_partition_id()
  #   |> parallel_each(fn {partition_id, v_nodes} ->
  #     node_names = Enum.map(v_nodes, fn %VNode{node_name: node_name} -> node_name end)

  #     :ok =
  #       cluster_state
  #       |> ClusterState.cluster_name()
  #       |> raft_group_id(partition_id)
  #       |> Raft.destroy_group(node_names)
  #   end)
  # end

  # #
  # # TODO: parallelize?
  # #
  def start_partition_members(cluster_state) do
    cluster_name = ClusterState.cluster_name(cluster_state)

    cluster_state
    |> RaftGroup.known_group_ids()
    |> Enum.each(fn
      {:partition, partition_id} ->
        Logger.info("Starting partition replica", cluster_name: cluster_name, partition_id: partition_id)

        :ok =
          partition_id
          |> raft_group_id()
          |> RaftGroup.restart_local_member(cluster_state)

      _ ->
        :noop
    end)
  end

  def command(cluster_state, partition_id, command) do
    command(
      cluster_state,
      partition_id,
      command,
      ClusterState.node_names(cluster_state, partition_id)
    )
  end

  def command(cluster_state, partition_id, command, nodes) do
    partition_id
    |> raft_group_id()
    |> RaftGroup.command(cluster_state, command, nodes)
  end

  defp query(cluster_state, partition_id, query) do
    nodes = ClusterState.node_names(cluster_state, partition_id)

    partition_id
    |> raft_group_id()
    |> RaftGroup.query(cluster_state, query, nodes)
  end

  def members(cluster_state, partition_id) do
    nodes = ClusterState.node_names(cluster_state, partition_id)

    partition_id
    |> raft_group_id()
    |> RaftGroup.members(cluster_state, nodes)
  end

  def leader(cluster_state, partition_id) do
    nodes = ClusterState.node_names(cluster_state, partition_id)

    partition_id
    |> raft_group_id()
    |> RaftGroup.leader(cluster_state, nodes)
  end

  def status(cluster_state, partition_id) do
    nodes = ClusterState.node_names(cluster_state, partition_id)

    partition_id
    |> raft_group_id()
    |> RaftGroup.query(cluster_state, {PartitionMachine, :status_fun, []}, nodes)
  end

  def status(cluster_state) do
    cluster_state
    |> ClusterState.partition_ids()
    |> Enum.into(%{}, fn partition_id ->
      # TODO: catch timeouts/errors
      {leader, members} = members(cluster_state, partition_id)
      {:ok, partition_status} = status(cluster_state, partition_id)

      status = %{
        status: partition_status,
        members: members,
        leader: leader
      }

      {partition_id, status}
    end)
  end

  def raft_group_id(cluster_state, partition_id) do
    RaftGroup.raft_group_id(cluster_state, raft_group_id(partition_id))
  end

  def raft_group_id(partition_id) do
    {:partition, partition_id}
  end
end
