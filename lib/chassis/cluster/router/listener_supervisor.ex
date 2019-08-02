defmodule Chassis.Cluster.Router.ListenerSupervisor do
  @moduledoc false

  use DynamicSupervisor

  alias Chassis.Cluster.Router.Listener

  def start_link([]) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init([]) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_listener(cluster_name) do
    DynamicSupervisor.start_child(__MODULE__, {Listener, cluster_name})
  end
end
