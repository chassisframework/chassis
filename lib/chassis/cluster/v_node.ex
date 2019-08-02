defmodule Chassis.Cluster.VNode do
  @moduledoc false

  alias Chassis.Cluster.Membership
  alias Chassis.Cluster.RequestRouter
  alias Chassis.ClusterState
  alias Chassis.ClusterState.Config
  alias Chassis.Request
  alias Chassis.Reply

  use GenServer

  defmacro __using__(_opts) do
    quote do
      # use GenServer

      # TODO: maybe allow for a default routing strategy?

      # import unquote(UserFunctions)
    end
  end

  @chassis_private_store_key "$chassis$"

  defmodule State do
    defstruct [
      :partition_id,
      :module,
      :user_state,
      # raft state
      :role,
      :transition_ranges
    ]
  end

  defmodule Dictionary do
    defstruct [
      :cluster_state,
      :partition_id
    ]
  end

  #
  # VNode User Functions
  #

  def state_command(name, command, context) do
    %Dictionary{cluster_state: cluster_state} = get_dictionary()

    request = %Request{
      Request.new(command, cluster_state, context)
      | state_app_name: name
    }

    cluster_state
    |> ClusterState.state_app_module(name)
    |> apply(:command, [cluster_state, request])
  end

  # TODO: raise when use provides both a key and a partition id
  def state_query(name, query, context) do
    %Dictionary{cluster_state: cluster_state} = get_dictionary()

    request = %Request{
      Request.new(query, cluster_state, context)
      | state_app_name: name
    }

    cluster_state
    |> ClusterState.state_app_module(name)
    |> apply(:query, [cluster_state, request])
  end

  #
  # Internal functions
  #

  def set_role(cluster_state, partition_id, role) do
    cluster_state
    |> ClusterState.cluster_name()
    |> name(partition_id)
    |> GenServer.cast({:"$Chassis_update_role$", role})
  end

  def release_range(cluster_name, partition_id, range, ranges) do
    cluster_name
    |> name(partition_id)
    |> GenServer.cast({:"$Chassis_release_range$", range, ranges})
  end

  def update_transition_ranges(cluster_name, partition_id, transition_ranges) do
    cluster_name
    |> name(partition_id)
    |> GenServer.cast({:"$Chassis_update_transition_ranges$", transition_ranges})
  end

  def transition_finished(cluster_name, partition_id, new_cluster_state) do
    cluster_name
    |> name(partition_id)
    |> GenServer.cast({:"$Chassis_transition_finished$", new_cluster_state})
  end

  #
  # GenServer bits
  #

  def start_link([cluster_state, partition_id, transition_ranges]) do
    name =
      cluster_state
      |> ClusterState.cluster_name()
      |> name(partition_id)

    GenServer.start_link(__MODULE__, [cluster_state, partition_id, transition_ranges], name: name)
  end

  @impl true
  def init([
        %ClusterState{
          config: %Config{
            cluster_name: cluster_name,
            v_node_app: {module, args}
          }
        } = cluster_state,
        partition_id,
        transition_ranges
      ]) do
    Logger.metadata(cluster_name: cluster_name, partition_id: partition_id)

    %Dictionary{cluster_state: cluster_state, partition_id: partition_id}
    |> put_dictionary()

    state = %State{
      module: module,
      partition_id: partition_id,
      transition_ranges: transition_ranges
    }

    range =
      if transition_ranges do
        nil
      else
        ClusterState.range_for_partition_id(cluster_state, partition_id)
      end

    if Kernel.function_exported?(module, :init, 2) do
      partition_context = %{
        partition_id: partition_id,
        range: range,
        cluster_state: cluster_state
      }

      module.init(args, partition_context)
    else
      module.init(args)
    end
    |> maybe_wrap_state(state)
  end

  @impl true
  def handle_call(
        %Request{request: user_request, context: request_context} = request,
        from,
        %State{
          partition_id: partition_id,
          module: module,
          user_state: user_state,
          transition_ranges: transition_ranges
        } = state
      ) do
    %Dictionary{cluster_state: cluster_state} = get_dictionary()

    case RequestRouter.should_handle_or_forward(cluster_state, transition_ranges, request, partition_id) do
      :handle ->
        request_context = Map.put(request_context, :from, from)

        user_request
        |> module.handle_call(request_context, user_state)
        |> maybe_wrap_state(state)
        |> wrap_reply()

      forward_or_error ->
        {:reply, forward_or_error, state}
    end
  end

  @impl true
  def handle_cast(
        %Request{request: user_request, context: request_context} = request,
        %State{
          partition_id: partition_id,
          module: module,
          user_state: user_state,
          transition_ranges: transition_ranges
        } = state
      ) do
    %Dictionary{cluster_state: cluster_state} = get_dictionary()

    case RequestRouter.should_handle_or_forward(cluster_state, transition_ranges, request, partition_id) do
      :handle ->
        user_request
        |> module.handle_cast(request_context, user_state)
        |> maybe_wrap_state(state)

      forward_or_error ->
        {:reply, forward_or_error, state}
    end
  end

  def handle_cast({:"$Chassis_update_role$", role}, state) do
    {:noreply, %State{state | role: role}}
  end

  def handle_cast({:"$Chassis_transition_finished$", new_cluster_state}, state) do
    get_dictionary()
    |> struct(cluster_state: new_cluster_state, transition_ranges: nil)
    |> put_dictionary()

    {:noreply, state}
  end

  def handle_cast({:"$Chassis_release_range$", range, transition_ranges}, %State{module: module, user_state: user_state} = state) do
    inform_subscribers(state, {:releasing_range, range})

    %Dictionary{cluster_state: cluster_state} = get_dictionary()

    {:ok, user_state} = module.release_range(range, cluster_state, user_state)

    state = %State{state | user_state: user_state, transition_ranges: transition_ranges}

    {:noreply, state}
  end

  def handle_cast({:"$Chassis_update_transition_ranges$", transition_ranges}, state) do
    inform_subscribers(state, {:updating_transition_ranges, transition_ranges})

    {:noreply, %State{state | transition_ranges: transition_ranges}}
  end

  @impl true
  def handle_info(msg, %State{module: module, user_state: user_state} = state) do
    msg
    |> module.handle_info(user_state)
    |> maybe_wrap_state(state)
  end

  @impl true
  def handle_continue(continue, %State{module: module, user_state: user_state} = state) do
    continue
    |> module.handle_continue(user_state)
    |> maybe_wrap_state(state)
  end

  @impl true
  def code_change(old_vsn, %State{module: module, user_state: user_state} = state, extra) do
    old_vsn
    |> module.code_change(user_state, extra)
    |> maybe_wrap_state(state)
  end

  @impl true
  def format_status(reason, [pdict, %State{module: module, user_state: user_state}]) do
    module.format_status(reason, [pdict, user_state])
  end

  @impl true
  def terminate(reason, %State{module: module, user_state: user_state}) do
    module.terminate(reason, user_state)
  end

  @doc false
  def name(cluster_name, partition_id) do
    Module.concat([__MODULE__, cluster_name, inspect(partition_id)])
  end

  @doc false
  def put_dictionary(dictionary) do
    Process.put(@chassis_private_store_key, dictionary)
  end

  @doc false
  def get_dictionary do
    Process.get(@chassis_private_store_key)
  end

  defp wrap_reply({:reply, reply, state}) do
    {:reply, do_wrap_reply(state, reply), state}
  end

  defp wrap_reply({:reply, reply, state, term}) do
    {:reply, do_wrap_reply(state, reply), state, term}
  end

  defp do_wrap_reply(%State{partition_id: partition_id}, reply) do
    %Reply{reply: reply, from_partition_id: partition_id}
  end

  defp wrap_state(%State{} = state, user_state) do
    %State{state | user_state: user_state}
  end

  defp maybe_wrap_state({:ok, user_state}, state) do
    {:ok, wrap_state(state, user_state)}
  end

  defp maybe_wrap_state({:ok, user_state, thing}, state) do
    {:ok, wrap_state(state, user_state), thing}
  end

  defp maybe_wrap_state({:noreply, user_state}, state) do
    {:noreply, wrap_state(state, user_state)}
  end

  defp maybe_wrap_state({:noreply, user_state, thing}, state) do
    {:noreply, wrap_state(state, user_state), thing}
  end

  defp maybe_wrap_state({:reply, reply, user_state}, state) do
    {:reply, reply, wrap_state(state, user_state)}
  end

  defp maybe_wrap_state({:reply, reply, user_state, thing}, state) do
    {:reply, reply, wrap_state(state, user_state), thing}
  end

  defp maybe_wrap_state({:stop, reason, user_state}, state) do
    {:stop, reason, wrap_state(state, user_state)}
  end

  defp maybe_wrap_state({:stop, reason, reply, user_state}, state) do
    {:stop, reason, reply, wrap_state(state, user_state)}
  end

  defp maybe_wrap_state(other, _state), do: other

  defp inform_subscribers(%State{partition_id: partition_id}, event) do
    %Dictionary{cluster_state: cluster_state} = get_dictionary()

    {:ok, cluster_state} =
      cluster_state
      |> ClusterState.cluster_name()
      |> Membership.get_state()

    ClusterState.inform_subscribers(cluster_state, {:v_node, partition_id, node()}, event)
  end
end
