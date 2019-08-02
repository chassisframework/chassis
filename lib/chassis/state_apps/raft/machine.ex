defmodule Chassis.RaftStateApp.Machine do
  #   @moduledoc false

  require Logger

  import Chassis.Raft, only: [async_effect: 4]

  #   alias Chassis.Cluster.Application.VNodeSupervisor
  #   alias Chassis.Cluster.Membership
  #   alias Chassis.Cluster.RangeAccept
  #   alias Chassis.Cluster.RangeRelease
  #   alias Chassis.Cluster.RangeSteal
  #   alias Chassis.Cluster.TransitionRanges
  alias Chassis.Cluster.RequestRouter
  #   alias Chassis.Cluster.Replica.Consistent, as: ConsistentReplica
  alias Chassis.ClusterState
  alias Chassis.Cluster.Partition
  #   alias Chassis.ClusterState.Config
  #   alias Chassis.Cluster.VNode
  alias Chassis.Request
  alias Chassis.RaftStateApp

  defmodule State do
    defstruct [
      :cluster_state,
      :partition_id,
      :name,
      # :initializing | :running | :stealing | :stealing | :releasing
      {:state, :initializing},
      :user_mod_and_args,
      :user_state,
      :transition_ranges
    ]
  end

  def init(%{
        app_args: %{
          cluster_state: cluster_state,
          name: name,
          partition_id: partition_id,
          transition_ranges: transition_ranges,
          user_mod_and_args: {module, args} = user_mod_and_args
        }
      }) do
    cluster_name = ClusterState.cluster_name(cluster_state)

    Logger.metadata(
      cluster_name: cluster_name,
      partition_id: partition_id,
      name: name
    )

    {status, range} =
      if transition_ranges do
        {:stealing, nil}
      else
        range = ClusterState.range_for_partition_id(cluster_state, partition_id)

        {:initializing, range}
      end

    state = %State{
      cluster_state: cluster_state,
      partition_id: partition_id,
      name: name,
      state: status,
      transition_ranges: transition_ranges,
      user_mod_and_args: user_mod_and_args
    }

    if Kernel.function_exported?(module, :init, 2) do
      context = %{
        name: name,
        partition_id: partition_id,
        range: range,
        cluster_state: cluster_state
      }

      module.init(args, context)
    else
      module.init(args)
    end
    |> case do
      {:ok, user_state} ->
        wrap_state(state, user_state)

      other ->
        raise "state module's init function must return {:ok, state}, got: #{inspect(other)}"
    end
  end

  def state_enter(:leader, %State{state: :initializing} = state) do
    [command_effect(:set_running_and_notify, state)]
  end

  def state_enter(_role, _state), do: []

  #   def state_enter(:leader, %State{state: :stealing} = state) do
  #     [command_effect(:steal_next_range, state)]
  #   end

  #   def state_enter(
  #     :recovered,
  #     %State{cluster_state: cluster_state, partition_id: partition_id, transition_ranges: transition_ranges} = state
  #   ) do
  #     # may need to async this
  #     cluster_state
  #     |> VNodeSupervisor.start_v_node(partition_id, transition_ranges)
  #     |> case do
  #          {:ok, _pid} ->
  #            :ok

  #          {:error, {:already_started, _pid}} ->
  #            # restart the vnode?
  #            :ok
  #        end

  #     set_v_node_role(:recovered, state)

  #     []
  #   end

  #   def apply(_meta, :get, state) do
  #     {state, state}
  #   end

  def apply(_meta, :set_running_and_notify, %State{state: :initializing} = state) do
    {%State{state | state: :running}, :ok, notify_partition_ready_effect(state)}
  end

  #   def apply(
  #     _meta,
  #     :steal_next_range,
  #     %State{
  #       state: :stealing,
  #       transition_ranges: transition_ranges
  #     } = state
  #   ) do
  #     {transition_ranges, range_steal} = TransitionRanges.steal_next_range(transition_ranges)
  #     state = %State{state | transition_ranges: transition_ranges}

  #     effects =
  #       case range_steal do
  #         nil ->
  #           [inform_subscribers_effect(state, :finished_stealing),
  #            confirm_transition_command_effect(state)]

  #         %RangeSteal{} ->
  #           [inform_subscribers_effect(state, {:stealing_range, range_steal}),
  #            steal_range_effect(state, range_steal)]
  #       end

  #     {state, :ok, effects}
  #   end

  #   def apply(
  #     _meta,
  #     {:transition_finished, new_cluster_state},
  #     %State{
  #       cluster_state: %ClusterState{config: %Config{cluster_name: cluster_name}},
  #       partition_id: partition_id,
  #       task_supervisor: task_supervisor
  #     } = state) do
  #     state = %State{state | state: :running, cluster_state: new_cluster_state, transition_ranges: nil}

  #     #
  #     # effects are only executed by the leader, we need every vnode to release the range
  #     #
  #     Task.Supervisor.async(
  #       task_supervisor,
  #       VNode,
  #       :transition_finished,
  #       [cluster_name,
  #        partition_id,
  #        new_cluster_state]
  #     )

  #     effects = [
  #       inform_subscribers_effect(state, :finished_transition)
  #     ]

  #     {state, :ok, effects}
  #   end

  #   def apply(
  #     meta,
  #     {:release_range, _range_release} = release_command,
  #     %State{
  #       transition_ranges: nil,
  #       cluster_state: cluster_state,
  #       partition_id: partition_id
  #     } = state
  #   ) do
  #     transition_ranges =
  #       cluster_state
  #       |> ClusterState.range_for_partition_id(partition_id)
  #       |> TransitionRanges.new()

  #     __MODULE__.apply(meta, release_command, %State{state | transition_ranges: transition_ranges})
  #   end

  #   def apply(
  #     _meta,
  #     {:release_range, %RangeRelease{range: range} = range_release},
  #     %State{
  #       transition_ranges: transition_ranges,
  #       cluster_state: %ClusterState{config: %Config{state_app: {module, _args}}} = cluster_state,
  #       partition_id: partition_id,
  #       user_state: user_state
  #     } = state
  #   ) do
  #     {:ok, range_data, user_state} = module.release_range(range, cluster_state, user_state)

  #     transition_ranges = TransitionRanges.release_range(transition_ranges, range_release)

  #     state =
  #       %State{
  #         state |
  #         state: :releasing,
  #         transition_ranges: transition_ranges,
  #         user_state: user_state
  #       }

  #     range_accept =
  #       %RangeAccept{
  #         range: range,
  #         from_partition_id: partition_id,
  #         data: range_data
  #       }

  #     effects = [
  #       inform_subscribers_effect(state, {:sending_range, range_accept}),
  #       send_range_effect(state, range_release, range_accept)
  #     ]

  #     {state, :ok, effects}
  #   end

  #   def apply(
  #     _meta,
  #     {:accept_range,
  #      %RangeAccept{
  #        range: range,
  #        from_partition_id: from_partition_id,
  #        data: range_data
  #      } = range_accept
  #     },
  #     %State{
  #       state: :stealing,
  #       cluster_state: %ClusterState{config: %Config{cluster_name: cluster_name,
  #                                                    state_app: {module, _args}}} = cluster_state,
  #       transition_ranges: %TransitionRanges{
  #         stealing: %RangeSteal{
  #           from_partition_id: from_partition_id,
  #           range: range
  #         }
  #       } = transition_ranges,
  #       partition_id: partition_id,
  #       task_supervisor: task_supervisor,
  #       user_state: user_state
  #     } = state
  #   ) do
  #     {:ok, user_state} = module.accept_range(range, range_data, cluster_state, user_state)

  #     transition_ranges = TransitionRanges.accept_range(transition_ranges, range_accept)

  #     # effects are only executed by the leader, we need every vnode to release the range
  #     #
  #     # this may need to be synchronous to ensure that the vnode has updated ranges before we
  #     # acknowledge to the source that this partition's ranges are updated.
  #     #
  #     # alternatively, instead of making this synchronous, we can send an async message to the vnode,
  #     # and then upon receipt of an :updated_ranges message from the vnode, we'd tell the source partition
  #     # that we've accepted the range.
  #     #
  #     Task.Supervisor.async(
  #       task_supervisor,
  #       VNode,
  #       :update_transition_ranges,
  #       [cluster_name,
  #        partition_id,
  #        transition_ranges]
  #     )

  #     state = %State{state | user_state: user_state, state: :stealing, transition_ranges: transition_ranges}
  #     effects = [
  #       inform_subscribers_effect(state, {:accepted_range, range_accept}),
  #       command_effect(:steal_next_range, state)
  #     ]

  #     {state, :ok, effects}
  #   end

  #   def apply(
  #     _meta,
  #     {:range_accepted, range},
  #     %State{
  #       transition_ranges: transition_ranges,
  #       task_supervisor: task_supervisor,
  #       cluster_state: %ClusterState{config: %Config{cluster_name: cluster_name}},
  #       partition_id: partition_id
  #     } = state
  #   ) do
  #     transition_ranges = TransitionRanges.range_accepted(transition_ranges, range)

  #     state =
  #       %State{
  #         state |
  #         state: :releasing,
  #         transition_ranges: transition_ranges
  #       }

  #     # effects are only executed by the leader, we need every vnode to release the range
  #     Task.Supervisor.async(
  #       task_supervisor,
  #       VNode,
  #       :release_range,
  #       [cluster_name,
  #        partition_id,
  #        range,
  #        transition_ranges]
  #     )

  #     {state, :ok, []}
  #   end

  def apply(
        _meta,
        %Request{request: user_request, context: user_context} = request,
        %State{
          partition_id: partition_id,
          transition_ranges: transition_ranges,
          user_state: user_state,
          user_mod_and_args: {module, _args},
          cluster_state: cluster_state
        } = state
      ) do
    case RequestRouter.should_handle_or_forward(cluster_state, transition_ranges, request, partition_id) do
      :handle ->
        user_request
        |> module.handle_command(user_context, user_state)
        |> maybe_wrap_state(state)

      forward_or_error ->
        {:reply, forward_or_error, state}
    end
  end

  def apply(_meta, command, state) do
    Logger.warn("unexpected command #{inspect(command)} in partition #{inspect(state)}")

    {state, {:error, :unknown_command}}
  end

  def user_query_fn(
        %Request{
          request: user_request,
          context: user_context
        } = request,
        %State{
          partition_id: partition_id,
          transition_ranges: transition_ranges,
          user_state: user_state,
          user_mod_and_args: {module, _args},
          cluster_state: cluster_state
        } = state
      ) do
    case RequestRouter.should_handle_or_forward(cluster_state, transition_ranges, request, partition_id) do
      :handle ->
        user_request
        |> module.handle_query(user_context, user_state)
        |> case do
          {:reply, reply} ->
            reply
        end

      forward_or_error ->
        forward_or_error
    end
  end

  #   def status_fun do
  #     fn %State{state: state} -> state end
  #   end

  defp maybe_wrap_state({:reply, reply, user_state}, state) do
    {wrap_state(state, user_state), reply}
  end

  defp wrap_state(%State{} = state, user_state) do
    %State{state | user_state: user_state}
  end

  #   defp set_v_node_role(
  #     role,
  #     %State{
  #       task_supervisor: task_supervisor,
  #       cluster_state: %ClusterState{config: %Config{cluster_name: cluster_name}},
  #       partition_id: partition_id
  #     }
  #   ) do
  #     Task.Supervisor.async(task_supervisor, VNode, :set_role, [cluster_name, partition_id, role])
  #   end

  defp notify_partition_ready_effect(%State{
         cluster_state: cluster_state,
         name: name,
         partition_id: partition_id
       }) do
    async_effect(cluster_state, Partition, :state_app_ready, [cluster_state, partition_id, name])
  end

  #   defp steal_range_effect(
  #     %State{
  #       task_supervisor: task_supervisor,
  #       cluster_state:
  #       %ClusterState{
  #         config: %Config{cluster_name: cluster_name},
  #         next_layout: next_layout
  #       } = cluster_state,
  #       partition_id: my_partition_id
  #     },
  #     %RangeSteal{
  #       from_partition_id: from_partition_id,
  #       range: range
  #     }
  #   ) do
  #     from_nodes = ClusterState.node_names(cluster_state, from_partition_id)

  #     # since the cluster state hasn't been updated on the source partition yet, we need to tell it what nodes are running our new partition
  #     my_nodes =
  #       ClusterState.node_names(%ClusterState{cluster_state | layout: next_layout}, my_partition_id)

  #     range_release =
  #       %RangeRelease{
  #         range: range,
  #         to_partition_id: my_partition_id,
  #         to_nodes: my_nodes
  #       }

  #     async_effect(
  #       task_supervisor,
  #       {ConsistentReplica, :command,
  #        [cluster_name, from_partition_id, {:release_range, range_release}, from_nodes]}
  #     )
  #   end

  #   defp send_range_effect(
  #     %State{
  #       task_supervisor: task_supervisor,
  #       cluster_state: %ClusterState{config: %Config{cluster_name: cluster_name}},
  #       partition_id: partition_id
  #     },
  #     %RangeRelease{
  #       range: range,
  #       to_partition_id: to_partition_id,
  #       to_nodes: to_nodes
  #     },
  #     %RangeAccept{
  #       range: range
  #     } = range_accept
  #   ) do
  #     async_effect(
  #       task_supervisor,
  #       cluster_name,
  #       {:partition, partition_id},
  #       {ConsistentReplica, :command,
  #        [cluster_name, to_partition_id, {:accept_range, range_accept}, to_nodes]},
  #       fn {:ok, :ok} -> {:range_accepted, range} end)
  #   end

  #   defp inform_subscribers_effect(
  #     %State{
  #       task_supervisor: task_supervisor,
  #       cluster_state: %ClusterState{config: %Config{cluster_name: cluster_name}},
  #       partition_id: partition_id
  #     },
  #     event
  #   ) do
  #     # our cached cluster state isn't going to contain an updated list of subscribers, so we fetch the current state
  #     {:ok, cluster_state} = Membership.get_state(cluster_name)

  #     async_effect(
  #       task_supervisor,
  #       {ClusterState, :inform_subscribers, [cluster_state, {:partition, partition_id}, event]}
  #     )
  #   end

  #   defp confirm_transition_command_effect(
  #     %State{
  #       task_supervisor: task_supervisor,
  #       cluster_state: %ClusterState{config: %Config{cluster_name: cluster_name}},
  #       confirmation_message: confirmation_message
  #     }
  #   ) do
  #     async_effect(
  #       task_supervisor,
  #       {Membership, :command, [cluster_name, {:transition, {:continue, confirmation_message}}]}
  #     )
  #   end

  defp command_effect(
         command,
         %State{
           cluster_state: cluster_state,
           partition_id: partition_id,
           name: name
         }
       ) do
    async_effect(
      cluster_state,
      RaftStateApp,
      :command,
      [
        cluster_state,
        name,
        partition_id,
        command
      ]
    )
  end
end
