defmodule Chassis.Cluster.RaftGroup do
  alias Chassis.Raft
  alias Chassis.ClusterState

  def start_group(group_id, cluster_state, machine, machine_args, nodes) do
    cluster_state
    |> raft_group_id(group_id)
    |> Raft.start_group(machine, machine_args, nodes)
  end

  def add_member(group_id, cluster_state, node, module, init_args, nodes) do
    cluster_state
    |> raft_group_id(group_id)
    |> Raft.add_member(node, module, init_args, nodes)
  end

  def destroy_group(group_id, cluster_state, nodes) do
    cluster_state
    |> raft_group_id(group_id)
    |> Raft.destroy_group(nodes)
  end

  def command(group_id, cluster_state, command, nodes) do
    cluster_state
    |> raft_group_id(group_id)
    |> Raft.command(nodes, command)
  end

  def query(group_id, cluster_state_or_name, query, nodes) do
    cluster_state_or_name
    |> raft_group_id(group_id)
    |> Raft.leader_query(nodes, query)
  end

  def members(group_id, cluster_state, nodes) do
    {:ok, {leader, members}} =
      cluster_state
      |> raft_group_id(group_id)
      |> Raft.members(nodes)

    {leader, members}
  end

  def leader(group_id, cluster_state, nodes) do
    cluster_state
    |> raft_group_id(group_id)
    |> Raft.leader(nodes)
  end

  def restart_local_member(group_id, cluster_state) do
    cluster_state
    |> raft_group_id(group_id)
    |> Raft.restart_local_member()
  end

  def known_cluster_names do
    Raft.known_group_ids()
    |> Enum.map(fn {cluster_name, _} -> cluster_name end)
    |> Enum.uniq()
  end

  def known_group_ids(%ClusterState{} = cluster_state) do
    cluster_state
    |> ClusterState.cluster_name()
    |> known_group_ids()
  end

  def known_group_ids(cluster_name) do
    Raft.known_group_ids()
    |> Enum.flat_map(fn
      {^cluster_name, group_id} ->
        [group_id]

      _ ->
        []
    end)
  end

  def raft_group_id(%ClusterState{} = cluster_state, group_id) do
    {ClusterState.cluster_name(cluster_state), group_id}
  end

  def raft_group_id(cluster_name, group_id) do
    {cluster_name, group_id}
  end
end
