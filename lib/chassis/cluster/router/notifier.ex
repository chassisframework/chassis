defmodule Chassis.Cluster.Router.Notifier do
  use GenServer

  alias Chassis.Cluster.Router.Cache, as: RouterCache
  alias Chassis.Cluster.Router.Listener

  defmodule State do
    defstruct [
      {:listeners, MapSet.new()},
      :cluster_name
    ]
  end

  def update_listeners(cluster_name) do
    cluster_name
    |> name()
    |> GenServer.call(:send_updates)
  end

  def listen(cluster_name, node) do
    name = {name(cluster_name), node}

    try do
      GenServer.call(name, :add_listener)
    catch
      :exit, _ ->
        :error
    end
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
    {:ok, %State{cluster_name: cluster_name}}
  end

  @impl true
  def handle_call(:add_listener, {pid, _ref}, %State{cluster_name: cluster_name} = state) do
    Process.monitor(pid)

    send_update(cluster_name, pid)

    {:reply, :ok, add_listener(state, pid)}
  end

  def handle_call(:send_updates, _from, %State{cluster_name: cluster_name, listeners: listeners} = state) do
    # TODO: GenServer.abcast/3?
    Enum.each(listeners, &send_update(cluster_name, &1))

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, listener, _reason}, state) do
    {:noreply, remove_listener(state, listener)}
  end

  defp add_listener(%State{listeners: listeners} = state, listener) do
    %State{state | listeners: MapSet.put(listeners, listener)}
  end

  defp remove_listener(%State{listeners: listeners} = state, listener) do
    %State{state | listeners: MapSet.delete(listeners, listener)}
  end

  defp send_update(cluster_name, pid) do
    {:ok, state} = RouterCache.get(cluster_name)

    Listener.send_update(state, pid)
  end
end
