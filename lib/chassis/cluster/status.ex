defmodule Chassis.Cluster.Status do
  alias Chassis.Cluster
  alias Chassis.ClusterState
  alias Chassis.ClusterState.Config
  alias Chassis.Cluster.Partition
  alias Chassis.Cluster.Membership

  # FIXME: this should consult the routing table listener for cluster names, too (when running on client nodes, Cluster.cluster_names/1 is empty)
  def status do
    Cluster.cluster_names()
    |> Enum.into(%{}, fn cluster_name ->
      {:ok, state} = Membership.get_state(cluster_name, [node()])

      %ClusterState{
        state: cluster_status,
        staged_changes: staged_changes,
        config: %Config{
          strategy: strategy,
          v_node_app: v_node_app,
          state_apps: state_apps
        }
      } = state

      partitions = Partition.status(state)

      status = %{
        status: cluster_status,
        strategy: strategy,
        v_node_app: v_node_app,
        state_apps: state_apps,
        partitions: partitions,
        staged_changes: staged_changes
      }

      {cluster_name, status}
    end)
  end

  # def status do
  #   import Chassis.Util.Visualization, only: [center: 2, color: 2]
  #   # import IO.ANSI, only: []

  #     [
  #       " Cluster #{cluster_name |> inspect()} " |> center("="),
  #       "Status: #{cluster_status |> inspect() |> color(:green)}",
  #       "Configuration:",
  #       "  v_node_app: #{inspect v_node_app}",
  #       "  state_app: #{inspect state_app}",
  #       "    state_mode: #{inspect state_mode}",
  #       "  partitioning_strategy: #{inspect partitioning_strategy}",
  #       "Partition Status:",
  #     ]
  #     |> Enum.join("\n")
  #     |> IO.puts()

  #   end)
  # end
end
