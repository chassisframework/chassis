defmodule Chassis.Application do
  @moduledoc false

  use Application

  alias Chassis.Cluster.Membership
  alias Chassis.Cluster.ApplicationsSupervisor
  alias Chassis.Cluster.Router.ListenerSupervisor, as: RoutingListenerSupervisor

  def start(_type, _args) do
    children = [
      {ApplicationsSupervisor, []},
      {RoutingListenerSupervisor, []}
    ]

    Membership.start_known_membership_replicas()

    opts = [strategy: :one_for_one, name: Chassis.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
