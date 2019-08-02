# defmodule Chassis.DeltaCrdtStateApp do
#   alias Chassis.Cluster.Application.DeltaCrdtSupervisor
#   alias Chassis.ClusterState
#   alias Chassis.Request

#   def start(
#     cluster_state,
#     instance_name,
#     [],
#     partition_id,
#     nodes,
#     ready_message,
#     transition_ranges \\ nil
#   ) do
#     # :ok =

#       partition_id
#       |> consensus_group_id(instance_name)
#       # |> RaftGroup.start_group(cluster_state, Machine, machine_args, nodes)
#   end

#   def command(
#     cluster_state,
#     %Request{
#       state_app_name: state_app_name,
#       partition_id: partition_id
#     } = request
#   ) do
#     nodes = ClusterState.node_names(cluster_state, partition_id)

#     partition_id
#     |> consensus_group_id(state_app_name)
#     # |> RaftGroup.command(cluster_state, request, nodes)
#   end

#   def query(
#     cluster_state,
#     %Request{
#       state_app_name: state_app_name,
#       partition_id: partition_id
#     } = request
#   ) do
#     nodes = ClusterState.node_names(cluster_state, partition_id)
#     # query = {Machine, :user_query_fn, [request]}

#     partition_id
#     |> consensus_group_id(state_app_name)
#     # |> RaftGroup.query(cluster_state, query, nodes)
#   end

#   # def members(cluster_state, partition_id, instance_name) do
#   #   nodes = ClusterState.node_names(cluster_state, partition_id)

#   #   partition_id
#   #   |> consensus_group_id(instance_name)
#   #   |> RaftGroup.members(nodes)
#   # end

#   # def status(cluster_state, partition_id, instance_name) do
#   #   nodes = ClusterState.node_names(cluster_state, partition_id)

#   #   partition_id
#   #   |> consensus_group_id(instance_name)
#   #   |> RaftGroup.status(cluster_state, Machine.status_fun(), nodes)
#   # end

#   # def status(cluster_state) do
#   #   cluster_state
#   #   |> ClusterState.partition_ids()
#   #   |> Enum.into(%{}, fn partition_id ->
#   #     #TODO: catch timeouts/errors
#   #     {leader, members} = members(cluster_state, partition_id)
#   #     partition_status = status(cluster_state, partition_id)

#   #     status = %{
#   #       status: partition_status,
#   #       members: members,
#   #       leader: leader
#   #     }

#   #     {partition_id, status}
#   #   end)
#   # end

#   defp consensus_group_id(partition_id, name) do
#     {:delta_crdt, partition_id, name}
#   end
# end
