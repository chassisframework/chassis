# defmodule Chassis.DeltaCrdtStateApp.Supervisor do
#   use DynamicSupervisor

#   alias Chassis.ClusterState

#   def start_link(cluster_name) do
#     DynamicSupervisor.start_link(__MODULE__, [], name: name(cluster_name))
#   end

#   def start_child(cluster_name, partition_id, instance_name) do
#     spec = {DeltaCrdt, [crdt: DeltaCrdt.AWLWWMap, name: name(cluster_name, partition_id, instance_name)]}

#     DynamicSupervisor.start_child(name(cluster_name), spec)
#   end

#   @impl true
#   def init([]) do
#     DynamicSupervisor.init(
#       strategy: :one_for_one,
#       extra_arguments: []
#     )
#   end

#   defp name(cluster_name) do
#     Module.concat(__MODULE__, cluster_name)
#   end

#   defp name(cluster_name, partition_id, instance_name) do
#     Module.concat([
#       name(cluster_name),
#       inspect(partition_id),
#       instance_name
#     ])
#   end
# end
