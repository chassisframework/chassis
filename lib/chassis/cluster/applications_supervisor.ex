defmodule Chassis.Cluster.ApplicationsSupervisor do
  @moduledoc false

  alias Chassis.Cluster.Application.Supervisor, as: ApplicationSupervisor

  use DynamicSupervisor

  def start_link([]) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init([]) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  #
  # called from the raft membership group member
  #
  def start_user_application(state) do
    DynamicSupervisor.start_child(__MODULE__, {ApplicationSupervisor, [state, self()]})
  end

  def stop_user_application(cluster_name) do
    pid =
      cluster_name
      |> ApplicationSupervisor.name()
      |> Process.whereis()

    DynamicSupervisor.terminate_child(__MODULE__, pid)
  end
end
