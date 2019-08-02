defmodule Chassis.Cluster.Router.Listener do
  use GenServer, restart: :transient

  require Logger

  alias Chassis.ClusterState
  alias Chassis.ClusterState.Config
  alias Chassis.Cluster.Router.Notifier
  alias Chassis.Cluster.Router.Cache, as: RouterCache

  defmodule State do
    defstruct [:cluster_name, :node, {:initial_update_received?, false}, {:initial_notification_pids, []}]
  end

  # if multiple processes hit Router.maybe_start_listener/1 at the same time, only one of them
  # will successfully start the listener (and thus re)
  def await_initial_update(cluster_name, timeout \\ 5_000) do
    cluster_name
    |> name()
    |> GenServer.cast({:notify_when_ready, self()})

    msg = initial_update_ready_msg(cluster_name)

    receive do
      ^msg ->
        :ok
    after
      timeout ->
        raise "never received initial routing table for #{cluster_name}"
    end
  end

  def send_update(cluster_state, pid) do
    GenServer.cast(pid, {:update, cluster_state})
  end

  def name(cluster_name) do
    Module.concat(__MODULE__, cluster_name)
  end

  #
  # GenServer callbacks
  #

  def start_link(cluster_name) do
    GenServer.start_link(__MODULE__, cluster_name, name: name(cluster_name))
  end

  @impl true
  def init(cluster_name) do
    :ok = :net_kernel.monitor_nodes(true)

    {:ok, %State{cluster_name: cluster_name, initial_notification_pids: []}, {:continue, :listen}}
  end

  @impl true
  def handle_continue(:listen, %State{cluster_name: cluster_name} = state) do
    {:ok, node} = listen(cluster_name)

    {:noreply, %State{state | node: node}}
  end

  @impl true
  def handle_cast(
        {:update, %ClusterState{config: %Config{cluster_name: cluster_name}} = cluster_state},
        %State{cluster_name: cluster_name} = state
      ) do
    RouterCache.put(cluster_name, cluster_state)

    maybe_notify_initial_waiters(state)

    {:noreply, %State{state | initial_update_received?: true, initial_notification_pids: []}}
  end

  def handle_cast({:notify_when_ready, pid}, %State{initial_update_received?: true} = state) do
    send_ready(pid, state)

    {:noreply, state}
  end

  def handle_cast(
        {:notify_when_ready, pid},
        %State{initial_update_received?: false, initial_notification_pids: initial_notification_pids} = state
      ) do
    {:noreply, %State{state | initial_notification_pids: [pid | initial_notification_pids]}}
  end

  @impl true
  def handle_info({:nodedown, node}, %State{cluster_name: cluster_name, node: node} = state) do
    Logger.warn("[Chassis] Lost connection to routing table notifer, #{inspect(node)}, for cluster #{cluster_name}, restarting.", cluster_name: cluster_name)

    {:stop, :lost_connection, state}
  end

  def handle_info({:nodedown, _}, state), do: {:noreply, state}
  def handle_info({:nodeup, _}, state), do: {:noreply, state}

  #
  # `nodes` is just used as a set of bootstrap nodes, # once a node is successfully contacted,
  # the authoritative list of nodes in the cluster is taken from the router itself.
  #
  # that being said, scanning all the nodes in the cluster for one that's running our application sucks
  #
  defp listen(cluster_name, nodes \\ :erlang.nodes(:known))

  defp listen(cluster_name, [node | nodes]) do
    cluster_name
    |> Notifier.listen(node)
    |> case do
      :ok ->
        {:ok, node}

      :error ->
        listen(cluster_name, nodes)
    end
  end

  defp listen(_cluster_name, []) do
    {:error, :no_contactable_notifiers}
  end

  defp maybe_notify_initial_waiters(%State{initial_notification_pids: []}), do: :ok

  defp maybe_notify_initial_waiters(%State{cluster_name: cluster_name, initial_notification_pids: pids}) do
    Enum.each(pids, fn pid -> send(pid, initial_update_ready_msg(cluster_name)) end)
  end

  defp send_ready(pid, %State{cluster_name: cluster_name}) do
    send(pid, initial_update_ready_msg(cluster_name))
  end

  defp initial_update_ready_msg(cluster_name) do
    {"$Chassis_router_ready$", cluster_name}
  end
end
