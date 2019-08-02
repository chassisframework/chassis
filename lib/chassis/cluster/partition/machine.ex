defmodule Chassis.Cluster.Partition.Machine do
  @moduledoc false

  # use RaTransactionMachine, role: :coordinator
  # use RaTransactionMachine, role: :participant

  require Logger

  import Chassis.Raft, only: [async_effect: 4]

  alias Chassis.Cluster.Application.ClusterTaskSupervisor
  alias Chassis.Cluster.Application.VNodeSupervisor
  alias Chassis.Cluster.Membership
  alias Chassis.Cluster.Partition
  # alias Chassis.Cluster.RangeAccept
  # alias Chassis.Cluster.RangeRelease
  # alias Chassis.Cluster.RangeSteal
  # alias Chassis.Cluster.RequestRouter
  # alias Chassis.Cluster.TransitionRanges
  alias Chassis.Cluster.VNode
  alias Chassis.ClusterState
  # alias Chassis.ClusterState.Config
  # alias Chassis.Request

  defmodule State do
    defstruct [
      :partition_id,
      # {:initializing, state_apps_remaining} | :running | :stealing | :releasing
      :state,
      {:app_states, %{}},
      :cluster_state,
      :transition_ranges,
      :transactions,
      :confirmation_message
    ]
  end

  @impl true
  def raft_group_id(%State{cluster_state: cluster_state, partition_id: partition_id}) do
    Partition.raft_group_id(cluster_state, partition_id)
  end

  @impl true
  def put_transactions(%State{} = state, transactions) do
    %State{state | transactions: transactions}
  end

  @impl true
  def get_transactions(%State{transactions: transactions}) do
    transactions
  end

  def init(%{app_args: %{partition_id: partition_id, cluster_state: cluster_state, transition_ranges: transition_ranges, confirmation_message: confirmation_message}}) do
    status =
      if transition_ranges do
        :stealing
      else
        state_apps_remaining =
          cluster_state
          |> ClusterState.state_apps()
          |> Enum.map(fn {name, _} -> name end)

        {:initializing, state_apps_remaining}
      end

    %State{
      state: status,
      partition_id: partition_id,
      cluster_state: cluster_state,
      transition_ranges: transition_ranges,
      confirmation_message: confirmation_message
    }
  end

  def state_enter(:leader, %State{state: {:initializing, _state_apps_remaining}, partition_id: partition_id, cluster_state: cluster_state} = state) do
    nodes = ClusterState.node_names(cluster_state, partition_id)

    cluster_state
    |> ClusterState.state_apps()
    |> Enum.each(fn {name, {module, args}} ->
      ClusterTaskSupervisor.async(
        cluster_state,
        module,
        :start,
        [cluster_state, name, args, partition_id, nodes]
      )
    end)

    set_v_node_role(:leader, state)

    []
  end

  def state_enter(:leader, %State{state: :stealing} = state) do
    [command_effect(:steal_next_range, state)]
  end

  def state_enter(:leader, state) do
    set_v_node_role(:leader, state)

    []
  end

  def state_enter(:follower, state) do
    set_v_node_role(:follower, state)

    []
  end

  def state_enter(:recovered, %State{cluster_state: cluster_state, partition_id: partition_id, transition_ranges: transition_ranges} = state) do
    cluster_name = ClusterState.cluster_name(cluster_state)

    Logger.metadata(cluster_name: cluster_name, partition_id: partition_id)

    # may need to async this
    cluster_state
    |> VNodeSupervisor.start_v_node(partition_id, transition_ranges)
    |> case do
      {:ok, _pid} ->
        :ok

      {:error, {:already_started, _pid}} ->
        # restart the vnode?
        :ok
    end

    # TODO: ensure local instance of state apps recover here

    set_v_node_role(:recovered, state)

    []
  end

  def state_enter(role, state) do
    set_v_node_role(role, state)

    []
  end

  def apply(_meta, :get, state) do
    {state, state}
  end

  def apply(_meta, {:state_app_ready, name}, %State{state: {:initializing, [name]}} = state) do
    {%State{state | state: :running}, :ok, notify_init_finished_effect(state)}
  end

  def apply(_meta, {:state_app_ready, name}, %State{state: {:initializing, state_apps_remaining}} = state) do
    state = %State{state | state: {:initializing, List.delete(state_apps_remaining, name)}}

    {state, :ok}
  end

  # def apply(
  #   _meta,
  #   :steal_next_range,
  #   %State{
  #     state: :stealing,
  #     transition_ranges: transition_ranges
  #   } = state
  # ) do
  #   {transition_ranges, range_steal} = TransitionRanges.steal_next_range(transition_ranges)
  #   state = %State{state | transition_ranges: transition_ranges}

  #   effects =
  #     case range_steal do
  #       nil ->
  #         [inform_subscribers_effect(state, :finished_stealing),
  #          confirm_transition_command_effect(state)]

  #       %RangeSteal{} ->
  #         [inform_subscribers_effect(state, {:stealing_range, range_steal}),
  #          steal_range_effect(state, range_steal)]
  #     end

  #   {state, :ok, effects}
  # end

  # def apply(
  #   _meta,
  #   {:transition_finished, new_cluster_state},
  #   %State{
  #     cluster_state: %ClusterState{config: %Config{cluster_name: cluster_name}},
  #     partition_id: partition_id,
  #     task_supervisor: task_supervisor
  #   } = state) do
  #   state = %State{state | state: :running, cluster_state: new_cluster_state, transition_ranges: nil}

  #   #
  #   # effects are only executed by the leader, we need every vnode to release the range
  #   #
  #   Task.Supervisor.async(
  #     task_supervisor,
  #     VNode,
  #     :transition_finished,
  #     [cluster_name,
  #      partition_id,
  #      new_cluster_state]
  #   )

  #   effects = [
  #     inform_subscribers_effect(state, :finished_transition)
  #   ]

  #   {state, :ok, effects}
  # end

  # def apply(
  #   meta,
  #   {:release_range, _range_release} = release_command,
  #   %State{
  #     transition_ranges: nil,
  #     cluster_state: cluster_state,
  #     partition_id: partition_id
  #   } = state
  # ) do
  #   transition_ranges =
  #     cluster_state
  #     |> ClusterState.range_for_partition_id(partition_id)
  #     |> TransitionRanges.new()

  #   __MODULE__.apply(meta, release_command, %State{state | transition_ranges: transition_ranges})
  # end

  # def apply(
  #   _meta,
  #   {:release_range, %RangeRelease{range: range} = range_release},
  #   %State{
  #     transition_ranges: transition_ranges,
  #     cluster_state: %ClusterState{config: %Config{state_app: {module, _args}}} = cluster_state,
  #     partition_id: partition_id,
  #     user_state: user_state
  #   } = state
  # ) do
  #   {:ok, range_data, user_state} = module.release_range(range, cluster_state, user_state)

  #   transition_ranges = TransitionRanges.release_range(transition_ranges, range_release)

  #   state =
  #     %State{
  #       state |
  #       state: :releasing,
  #       transition_ranges: transition_ranges,
  #       user_state: user_state
  #     }

  #   range_accept =
  #     %RangeAccept{
  #       range: range,
  #       from_partition_id: partition_id,
  #       data: range_data
  #     }

  #   effects = [
  #     inform_subscribers_effect(state, {:sending_range, range_accept}),
  #     send_range_effect(state, range_release, range_accept)
  #   ]

  #   {state, :ok, effects}
  # end

  # def apply(
  #   _meta,
  #   {:accept_range,
  #    %RangeAccept{
  #      range: range,
  #      from_partition_id: from_partition_id,
  #      data: range_data
  #    } = range_accept
  #   },
  #   %State{
  #     state: :stealing,
  #     cluster_state: %ClusterState{config: %Config{cluster_name: cluster_name,
  #                                                  state_app: {module, _args}}} = cluster_state,
  #     transition_ranges: %TransitionRanges{
  #       stealing: %RangeSteal{
  #         from_partition_id: from_partition_id,
  #         range: range
  #       }
  #     } = transition_ranges,
  #     partition_id: partition_id,
  #     task_supervisor: task_supervisor,
  #     user_state: user_state
  #   } = state
  # ) do
  #   {:ok, user_state} = module.accept_range(range, range_data, cluster_state, user_state)

  #   transition_ranges = TransitionRanges.accept_range(transition_ranges, range_accept)

  #   # effects are only executed by the leader, we need every vnode to release the range
  #   #
  #   # this may need to be synchronous to ensure that the vnode has updated ranges before we
  #   # acknowledge to the source that this partition's ranges are updated.
  #   #
  #   # alternatively, instead of making this synchronous, we can send an async message to the vnode,
  #   # and then upon receipt of an :updated_ranges message from the vnode, we'd tell the source partition
  #   # that we've accepted the range.
  #   #
  #   Task.Supervisor.async(
  #     task_supervisor,
  #     VNode,
  #     :update_transition_ranges,
  #     [cluster_name,
  #      partition_id,
  #      transition_ranges]
  #   )

  #   state = %State{state | user_state: user_state, state: :stealing, transition_ranges: transition_ranges}
  #   effects = [
  #     inform_subscribers_effect(state, {:accepted_range, range_accept}),
  #     command_effect(:steal_next_range, state)
  #   ]

  #   {state, :ok, effects}
  # end

  # def apply(
  #   _meta,
  #   {:range_accepted, range},
  #   %State{
  #     transition_ranges: transition_ranges,
  #     task_supervisor: task_supervisor,
  #     cluster_state: %ClusterState{config: %Config{cluster_name: cluster_name}},
  #     partition_id: partition_id
  #   } = state
  # ) do
  #   transition_ranges = TransitionRanges.range_accepted(transition_ranges, range)

  #   state =
  #     %State{
  #       state |
  #       state: :releasing,
  #       transition_ranges: transition_ranges
  #     }

  #   # effects are only executed by the leader, we need every vnode to release the range
  #   Task.Supervisor.async(
  #     task_supervisor,
  #     VNode,
  #     :release_range,
  #     [cluster_name,
  #      partition_id,
  #      range,
  #      transition_ranges]
  #   )

  #   {state, :ok, []}
  # end

  # def apply(
  #   _meta,
  #   {:user_command, %Request{request: user_request, context: user_context} = request},
  #   %State{
  #     partition_id: partition_id,
  #     transition_ranges: transition_ranges,
  #     user_state: user_state,
  #     cluster_state: %ClusterState{config: %Config{state_app: {module, _args}}} = cluster_state
  #   } = state
  # ) do
  #   case RequestRouter.should_handle_or_forward(cluster_state, transition_ranges, request, partition_id) do
  #     :handle ->
  #       user_request
  #       |> module.handle_command(user_context, user_state)
  #       |> maybe_wrap_state(state)

  #     forward_or_error ->
  #       {:reply, forward_or_error, state}
  #   end
  # end

  def apply(_meta, command, state) do
    Logger.warn("unexpected command #{inspect(command)} in partition #{inspect(state)}")

    {state, {:error, :unknown_command}}
  end

  # def user_query_fun(%Request{request: user_request, context: user_context} = request) do
  #   fn %State{
  #     partition_id: partition_id,
  #     transition_ranges: transition_ranges,
  #     user_state: user_state,
  #     cluster_state: %ClusterState{
  #       config: %Config{state_app: {module, _args}}
  #     } = cluster_state
  #  } = state ->
  #     case RequestRouter.should_handle_or_forward(cluster_state, transition_ranges, request, partition_id) do
  #       :handle ->
  #         user_request
  #         |> module.handle_query(user_context, user_state)
  #         |> case do
  #              {:reply, reply} ->
  #                reply
  #            end

  #       forward_or_error ->
  #         {:reply, forward_or_error, state}
  #     end

  #   end
  # end

  def status_fun(%State{state: state}) do
    state
  end

  # defp maybe_wrap_state({:reply, reply, user_state}, state) do
  #   {wrap_state(state, user_state), reply}
  # end

  # defp wrap_state(%State{} = state, user_state) do
  #   %State{state | user_state: user_state}
  # end

  defp set_v_node_role(
         role,
         %State{
           cluster_state: cluster_state,
           partition_id: partition_id
         }
       ) do
    ClusterTaskSupervisor.async(cluster_state, VNode, :set_role, [cluster_state, partition_id, role])
  end

  defp notify_init_finished_effect(%State{
         cluster_state: cluster_state,
         partition_id: partition_id
       }) do
    async_effect(cluster_state, Membership, :partition_ready, [cluster_state, partition_id])
  end

  # defp steal_range_effect(
  #   %State{
  #     task_supervisor: task_supervisor,
  #     cluster_state: %ClusterState{
  #       config: %Config{cluster_name: cluster_name},
  #       next_layout: next_layout
  #     } = cluster_state,
  #     partition_id: my_partition_id
  #   },
  #   %RangeSteal{
  #     from_partition_id: from_partition_id,
  #     range: range
  #   }
  # ) do
  #   from_nodes = ClusterState.node_names(cluster_state, from_partition_id)

  #   # since the cluster state hasn't been updated on the source partition yet, we need to tell it what nodes are running our new partition
  #   my_nodes =
  #     ClusterState.node_names(%ClusterState{cluster_state | layout: next_layout}, my_partition_id)

  #   range_release =
  #     %RangeRelease{
  #       range: range,
  #       to_partition_id: my_partition_id,
  #       to_nodes: my_nodes
  #     }

  #   async_effect(
  #     task_supervisor,
  #     {ConsistentReplica, :command,
  #      [cluster_name, from_partition_id, {:release_range, range_release}, from_nodes]}
  #   )
  # end

  # defp send_range_effect(
  #   %State{
  #     task_supervisor: task_supervisor,
  #     cluster_state: %ClusterState{config: %Config{cluster_name: cluster_name}},
  #     partition_id: partition_id
  #   },
  #   %RangeRelease{
  #     range: range,
  #     to_partition_id: to_partition_id,
  #     to_nodes: to_nodes
  #   },
  #   %RangeAccept{
  #     range: range
  #   } = range_accept
  # ) do
  #   async_effect(
  #     task_supervisor,
  #     cluster_name,
  #     {:partition, partition_id},
  #     {ConsistentReplica, :command,
  #      [cluster_name, to_partition_id, {:accept_range, range_accept}, to_nodes]},
  #     fn {:ok, :ok} -> {:range_accepted, range} end)
  # end

  defp inform_subscribers_effect(
         %State{
           cluster_state: cached_cluster_state,
           partition_id: partition_id
         },
         event
       ) do
    # our cached cluster state isn't going to contain an updated list of subscribers, so we fetch the current state
    {:ok, cluster_state} =
      cached_cluster_state
      |> ClusterState.cluster_name()
      |> Membership.get_state()

    async_effect(cluster_state, ClusterState, :inform_subscribers, [cluster_state, {:partition, partition_id}, event])
  end

  # defp confirm_transition_command_effect(
  #   %State{
  #     task_supervisor: task_supervisor,
  #     cluster_state: %ClusterState{config: %Config{cluster_name: cluster_name}},
  #     confirmation_message: confirmation_message
  #   }
  # ) do
  #   async_effect(
  #     task_supervisor,
  #     {Membership, :command, [cluster_name, {:transition, {:continue, confirmation_message}}]}
  #   )
  # end

  defp command_effect(
         command,
         %State{
           cluster_state: cluster_state,
           partition_id: this_partition_id
         },
         opts \\ []
       ) do
    partition_id = Keyword.get(opts, :partition_id, this_partition_id)
    nodes = Keyword.get_lazy(opts, :nodes, fn -> ClusterState.node_names(cluster_state, partition_id) end)

    async_effect(
      cluster_state,
      Partition,
      :command,
      [
        cluster_state,
        partition_id,
        command,
        nodes
      ]
    )
  end
end
