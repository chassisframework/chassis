#
# at some point, we should have one task supervisor per raft group
#
# TODO: add async_arq function to kick off an ARQ sender
#
defmodule Chassis.Cluster.Application.ClusterTaskSupervisor do
  alias Chassis.ClusterState

  def child_spec(cluster_name) do
    {Task.Supervisor, name: name(cluster_name)}
  end

  def async(cluster_state, m, f, a) do
    cluster_state
    |> ClusterState.cluster_name()
    |> name()
    |> Task.Supervisor.async(m, f, a)
  end

  defp name(cluster_name) do
    Module.concat(__MODULE__, cluster_name)
  end
end
