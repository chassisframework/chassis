defmodule KV do
  alias Chassis.RaftStateApp
  # alias Chassis.DeltaCrdtStateApp

  @state_apps %{
    :keys => {RaftStateApp, [{KV.State, []}]},
    # :stats => {DeltaCrdtStateApp, []}
  }

  def start(cluster_name, bootstrap_nodes) do
    Chassis.start_cluster(cluster_name, {KV.VNode, []}, @state_apps, bootstrap_nodes)
  end

  def start_local(cluster_name) do
    Chassis.start_local_cluster(cluster_name, {KV.VNode, []}, @state_apps)
  end

  def put(cluster_name, k, v) do
    Chassis.call(cluster_name, {:put, v}, key: k)
  end

  def get(cluster_name, k, opts \\ []) do
    opts = Keyword.put(opts, :key, k)

    Chassis.call(cluster_name, :get, opts)
  end

  def delete(cluster_name, k, opts \\ []) do
    opts = Keyword.put(opts, :key, k)

    Chassis.call(cluster_name, :delete, opts)
  end
end
