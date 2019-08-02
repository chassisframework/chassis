defmodule Chassis.RaftStateApp do
  alias Chassis.Cluster.RaftGroup
  alias Chassis.ClusterState
  alias Chassis.RaftStateApp.Machine
  alias Chassis.Request

  def start(
        cluster_state,
        name,
        [{_module, _module_args} = user_mod_and_args],
        partition_id,
        nodes,
        transition_ranges \\ nil
      ) do
    machine_args = %{
      cluster_state: cluster_state,
      name: name,
      partition_id: partition_id,
      transition_ranges: transition_ranges,
      user_mod_and_args: user_mod_and_args
    }

    :ok =
      partition_id
      |> raft_group_id(name)
      |> RaftGroup.start_group(cluster_state, Machine, machine_args, nodes)
  end

  def command(
        cluster_state,
        %Request{
          state_app_name: state_app_name,
          partition_id: partition_id
        } = request
      ) do
    command(cluster_state, state_app_name, partition_id, request)
  end

  def command(
        cluster_state,
        state_app_name,
        partition_id,
        command
      ) do
    nodes = ClusterState.node_names(cluster_state, partition_id)

    partition_id
    |> raft_group_id(state_app_name)
    |> RaftGroup.command(cluster_state, command, nodes)
  end

  def query(
        cluster_state,
        %Request{
          state_app_name: state_app_name,
          partition_id: partition_id
        } = request
      ) do
    nodes = ClusterState.node_names(cluster_state, partition_id)
    query = {Machine, :user_query_fn, [request]}

    partition_id
    |> raft_group_id(state_app_name)
    |> RaftGroup.query(cluster_state, query, nodes)
  end

  def members(cluster_state, partition_id, name) do
    nodes = ClusterState.node_names(cluster_state, partition_id)

    partition_id
    |> raft_group_id(name)
    |> RaftGroup.members(nodes)
  end

  def status(cluster_state, partition_id, name) do
    nodes = ClusterState.node_names(cluster_state, partition_id)

    partition_id
    |> raft_group_id(name)
    |> RaftGroup.status(cluster_state, Machine.status_fun(), nodes)
  end

  # def status(cluster_state) do
  #   cluster_state
  #   |> ClusterState.partition_ids()
  #   |> Enum.into(%{}, fn partition_id ->
  #     #TODO: catch timeouts/errors
  #     {leader, members} = members(cluster_state, partition_id)
  #     partition_status = status(cluster_state, partition_id)

  #     status = %{
  #       status: partition_status,
  #       members: members,
  #       leader: leader
  #     }

  #     {partition_id, status}
  #   end)
  # end

  defp raft_group_id(partition_id, name) do
    {:raft_partition_state, partition_id, name}
  end
end
