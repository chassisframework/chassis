defmodule Chassis.Cluster.Membership.Machine do
  @moduledoc false

  require Logger

  import Chassis.Raft, only: [async_effect: 4]

  alias Chassis.Cluster.Application.ClusterTaskSupervisor
  alias Chassis.Cluster.ApplicationsSupervisor
  alias Chassis.Cluster.Membership
  alias Chassis.Cluster.Membership.Changes.JoinChange
  alias Chassis.Cluster.Partition
  alias Chassis.Cluster.Router
  alias Chassis.Cluster.TransitionRanges
  alias Chassis.ClusterState
  alias Chassis.ClusterState.Config
  alias PartitionedClusterLayout.Diff.NewPartition
  alias PartitionedClusterLayout.Node
  alias PartitionedClusterLayout.Transition

  def state_query_function(state), do: state

  def init(%{app_args: nil}), do: :awaiting_initial_state

  #
  # this node is being added to the cluster, send confirmation message to the leader
  #
  def state_enter(_role, %ClusterState{state: {:transitioning, %Transition{current: {{:add_node, %Node{name: node}}, confirmation_message}}}} = state) when node == node() do
    # since effects are only executed by the leader, we have to explicitly perform these actions
    explicit_command(state, {:transition, {:continue, confirmation_message}})
    explicitly_inform_subscribers(state, {:node_ready, node()})

    []
  end

  def state_enter(:leader, %ClusterState{subscribers: subscribers} = state) do
    Logger.warn("Membership group failed over.")

    monitor_subscribers_effects =
      Enum.map(subscribers, fn subscriber ->
        {:monitor, :process, subscriber}
      end)

    [inform_subscribers_effect(state, {:leader, node()}) | monitor_subscribers_effects]
  end

  def state_enter(:follower, %ClusterState{} = state) do
    # followers don't execute effects, so we directly execute effects in another process
    explicitly_inform_subscribers(state, {:follower, node()})

    []
  end

  def state_enter(:recovered, %ClusterState{} = state), do: recover(state)

  def state_enter(:eol, %ClusterState{config: %Config{cluster_name: cluster_name}} = state) do
    Logger.info("Shutting down local VNodes for cluster #{inspect(cluster_name)}")

    :ok = ApplicationsSupervisor.stop_user_application(cluster_name)

    [inform_subscribers_effect(state, {:end_of_life, node()})]
  end

  def state_enter(_fsm_state, _state), do: []

  def apply(_meta, {:accept_initial_state, state}, :awaiting_initial_state) do
    _recovery_effects = recover(state)

    effects = [command_effect(:create_initial_partitions, state)]

    {state, :ok, effects}
  end

  def apply(_meta, {:subscribe, pid}, state) do
    state = ClusterState.add_subscriber(state, pid)
    effects = [{:monitor, :process, pid}]

    # this should probably be an effect
    Router.update_no_notify(state)

    {state, :ok, effects}
  end

  def apply(_meta, {:down, pid, _reason}, state) do
    {ClusterState.remove_subscriber(state, pid), :ok}
  end

  def apply(_meta, :create_initial_partitions, %ClusterState{state: :initializing} = state) do
    all_partitions = state |> ClusterState.partition_ids() |> MapSet.new()

    {%ClusterState{state | state: {:creating_partitions, all_partitions}}, :ok, [create_initial_partitions_effect(state)]}
  end

  def apply(_meta, {:partition_ready, partition_id}, %ClusterState{state: {:creating_partitions, remaining_partitions}, starter_pid: starter_pid} = state) do
    # TODO: better error
    if !MapSet.member?(remaining_partitions, partition_id) do
      raise "we weren't waiting for partition id #{inspect(partition_id)}!"
    end

    remaining_partitions = MapSet.delete(remaining_partitions, partition_id)

    if MapSet.size(remaining_partitions) < 1 do
      ready_msg =
        state
        |> ClusterState.cluster_name()
        |> Membership.cluster_ready_msg()

      {%ClusterState{state | state: :running}, :ok, [{:send_msg, :erlang.binary_to_term(starter_pid), ready_msg}]}
    else
      {%ClusterState{state | state: {:creating_partitions, remaining_partitions}}, :ok}
    end
  end

  #
  # stage a node join
  #
  def apply(_meta, {:stage, {:join, node, args}}, %ClusterState{state: :running, staged_changes: staged_changes} = state) do
    already_staged? =
      Enum.any?(staged_changes, fn
        %JoinChange{node: ^node} ->
          true

        _ ->
          false
      end)

    if already_staged? do
      {state, {:error, :already_staged, staged_changes}}
    else
      change = %JoinChange{node: node, args: args}
      staged_changes = [change | staged_changes]

      state = %ClusterState{state | staged_changes: staged_changes}
      effects = [inform_subscribers_effect(state, {:staged, change})]

      {state, {:ok, staged_changes}, effects}
    end
  end

  def apply(_meta, {:stage, :commit}, %ClusterState{state: :running, staged_changes: staged_changes} = state) when length(staged_changes) > 0 do
    effects = [inform_subscribers_effect(state, {:stage_commit, staged_changes}), compute_new_layout_effect(state)]

    {%ClusterState{state | state: :computing_new_layout}, :ok, effects}
  end

  def apply(_meta, {:install_layout_and_plan, {layout, plan}}, %ClusterState{state: :computing_new_layout, config: %Config{cluster_name: cluster_name}} = state) do
    Logger.info("cluster #{inspect(cluster_name)} installing new layout and plan")

    state = %ClusterState{state | next_layout: layout, state: {:transitioning, plan}}

    effects = [
      command_effect({:transition, :begin}, state),
      inform_subscribers_effect(state, {:beginning_transition, plan})
    ]

    {state, :ok, effects}
  end

  def apply(_meta, {:transition, :begin}, %ClusterState{state: {:transitioning, %Transition{current: current_step}}} = state) do
    effects =
      List.flatten([
        execute_transition_step_effects(current_step, state),
        inform_subscribers_effect(state, {:executing_transition_step, current_step})
      ])

    {state, :ok, effects}
  end

  def apply(_meta, {:transition, {:continue, confirmation_message}}, %ClusterState{state: {:transitioning, plan}} = state) do
    case Transition.advance(plan, confirmation_message) do
      {:error, error} ->
        {state, {:error, error}}

      {:ok, %Transition{current: :finished}} ->
        state = ClusterState.promote_next_layout(state)

        all_partitions = state |> ClusterState.partition_ids() |> MapSet.new()

        effects = [
          inform_subscribers_effect(state, :finalizing_transition)
          | finalize_transition_effects(state)
        ]

        state = %ClusterState{state | state: {:transition, {:finalizing, all_partitions}}}

        {state, :ok, effects}

      {:ok, %Transition{current: step} = plan} ->
        effects =
          List.flatten([
            execute_transition_step_effects(step, state),
            inform_subscribers_effect(state, {:executing_transition_step, step})
          ])

        state = %ClusterState{state | state: {:transitioning, plan}}

        {state, :ok, effects}
    end
  end

  def apply(_meta, {:transition, {:finalized, partition_id}}, %ClusterState{state: {:transition, {:finalizing, remaining_partitions}}} = state) do
    # TODO: better error
    if !MapSet.member?(remaining_partitions, partition_id) do
      raise "we weren't waiting for partition id #{inspect(partition_id)} to finalize!"
    end

    remaining_partitions = MapSet.delete(remaining_partitions, partition_id)

    effects = [
      inform_subscribers_effect(state, {:partition_finalized, partition_id})
    ]

    if MapSet.size(remaining_partitions) < 1 do
      state = %ClusterState{state | state: :running}

      effects = [
        inform_subscribers_effect(state, :transition_finished)
        | effects
      ]

      Router.update(state)

      {state, :ok, effects}
    else
      {%ClusterState{state | state: {:transition, {:finalizing, remaining_partitions}}}, :ok, effects}
    end
  end

  def apply(_meta, :get, state) do
    {state, state}
  end

  def apply(_meta, :destroy_cluster, state) do
    effects = [destroy_partitions_effect(state), destroy_membership_group_effect(state), inform_subscribers_effect(state, :destroying_cluster)]

    {%ClusterState{state | state: :destroying_cluster}, :ok, effects}
  end

  def apply(_meta, command, state) do
    Logger.warn("unexpected membership command #{inspect(command)} in cluster #{ClusterState.cluster_name(state)}, with state #{inspect(state.state)}")

    {state, {:error, :unknown_command}}
  end

  # defp set_state_effect(state, cluster_state) do
  #   command_effect({:set_state, state}, cluster_state)
  # end

  defp command_effect(command, state) do
    async_effect(state, Membership, :command, [state, command])
  end

  defp inform_subscribers_effect(state, event) do
    async_effect(state, ClusterState, :inform_subscribers, [state, :membership, event])
  end

  defp create_initial_partitions_effect(state) do
    async_effect(state, Partition, :create_initial_partitions, [state])
  end

  defp create_partition_effect(state, partition_id, v_nodes, transition_ranges, confirmation_message) do
    async_effect(state, Partition, :create_partition, [state, partition_id, v_nodes, transition_ranges, confirmation_message])
  end

  defp destroy_partitions_effect(state) do
    async_effect(state, Partition, :destroy_partitions, [state])
  end

  defp destroy_membership_group_effect(state) do
    async_effect(state, Membership, :destroy_membership_group, [state])
  end

  defp compute_new_layout_effect(state) do
    async_effect(state, __MODULE__, :do_compute_new_layout_effect, [state])
  end

  def do_compute_new_layout_effect(state) do
    next_layout_and_plan = ClusterState.compute_new_layout_and_plan(state)

    # TODO: ARQify
    state
    |> ClusterState.cluster_name()
    |> Membership.command({:install_layout_and_plan, next_layout_and_plan})
  end

  defp add_node_effect(%ClusterState{init_args: init_args} = state, node) do
    async_effect(state, Membership, :add_node, [state, node, __MODULE__, init_args])
  end

  defp execute_transition_step_effects({{:add_node, %Node{name: node}}, _conf_message}, state) do
    [
      command_effect({:transition, {:await_node, node}}, state),
      add_node_effect(state, node)
    ]
  end

  defp execute_transition_step_effects({{:add_partition, %NewPartition{id: partition_id, ranges: ranges, v_nodes: v_nodes}}, confirmation_message}, state) do
    transition_ranges = TransitionRanges.new(ranges)

    [
      create_partition_effect(state, partition_id, v_nodes, transition_ranges, confirmation_message),
      inform_subscribers_effect(state, {:creating_partition, partition_id, v_nodes, ranges, confirmation_message})
    ]
  end

  defp finalize_transition_effects(state) do
    state
    |> ClusterState.partition_ids()
    |> Enum.map(fn partition_id ->
      async_effect(state, __MODULE__, :finalize_partition_transition, [state, partition_id])
    end)
  end

  def finalize_partition_transition(state, partition_id) do
    state
    |> Partition.command(partition_id, {:transition_finished, state})
    |> case do
      {:ok, :ok} ->
        # TODO: ARQify
        state
        |> ClusterState.cluster_name()
        |> Membership.command({:transition, {:finalized, partition_id}})
    end
  end

  defp recover(state) do
    cluster_name = ClusterState.cluster_name(state)

    Logger.metadata(cluster_name: cluster_name)

    # may need to async all of these
    {:ok, _pid} = ApplicationsSupervisor.start_user_application(cluster_name)

    Router.update(state)

    Partition.start_partition_members(state)

    [inform_subscribers_effect(state, {:recovered, node()})]
  end

  defp explicitly_inform_subscribers(state, event) do
    ClusterTaskSupervisor.async(state, ClusterState, :inform_subscribers, [state, :membership, event])
  end

  defp explicit_command(state, command) do
    ClusterTaskSupervisor.async(state, Membership, :command, [ClusterState.cluster_name(state), command])
  end
end
