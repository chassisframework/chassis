defmodule Chassis.Cluster.Application.Supervisor do
  @moduledoc false

  use Supervisor, restart: :transient

  alias Chassis.Cluster.Application.VNodeSupervisor
  alias Chassis.Cluster.Application.ClusterTaskSupervisor
  # alias Chassis.DeltaCrdtStateApp.Supervisor, as: DeltaCrdtStateAppSupervisor
  alias Chassis.Cluster.Router.Notifier, as: RoutingNotifier

  def start_link([cluster_name, membership_pid]) do
    Supervisor.start_link(__MODULE__, [cluster_name, membership_pid], name: name(cluster_name))
  end

  @impl true
  def init([cluster_name, membership_pid]) do
    Process.monitor(membership_pid)

    [
      {VNodeSupervisor, cluster_name},
      {RoutingNotifier, cluster_name},
      ClusterTaskSupervisor.child_spec(cluster_name)
    ]
    # |> (fn children ->
    #   if Code.ensure_loaded?(DeltaCrdt) do
    #     [{DeltaCrdtStateAppSupervisor, cluster_name} | children]
    #   else
    #     children
    #   end
    # end).()
    |> Supervisor.init(strategy: :rest_for_one)
  end

  def name(cluster_name) do
    Module.concat(__MODULE__, cluster_name)
  end
end
