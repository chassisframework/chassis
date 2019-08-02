defmodule Chassis.Cluster.Router do
  @moduledoc false

  alias Chassis.Cluster.Router.Cache, as: RouterCache
  alias Chassis.Cluster.Router.Listener
  alias Chassis.Cluster.Router.ListenerSupervisor
  alias Chassis.Cluster.Router.Notifier
  alias Chassis.ClusterState

  def partition_for_key(cluster_name, key) do
    cluster_name
    |> get_state()
    |> ClusterState.partition_id_for_key(key)
  end

  def node_names(cluster_name) do
    cluster_name
    |> get_state()
    |> ClusterState.node_names()
  end

  def node_names(cluster_name, partition_id) do
    cluster_name
    |> get_state()
    |> ClusterState.node_names(partition_id)
  end

  def update(state) do
    cluster_name = ClusterState.cluster_name(state)

    RouterCache.put(cluster_name, state)

    Notifier.update_listeners(cluster_name)
  end

  def update_no_notify(state) do
    state
    |> ClusterState.cluster_name()
    |> RouterCache.put(state)
  end

  def subscribe(cluster_name) do
    {:ok, _pid} = ListenerSupervisor.start_listener(cluster_name)

    Listener.await_initial_update(cluster_name)

    :ok
  end

  def get_state(cluster_name) do
    case RouterCache.get(cluster_name) do
      {:ok, state} ->
        state

      {:error, :not_found} ->
        {:ok, _pid} = ListenerSupervisor.start_listener(cluster_name)
        Listener.await_initial_update(cluster_name)
        get_state(cluster_name)
    end
  end
end
