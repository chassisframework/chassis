defmodule Chassis.Request do
  @moduledoc false

  alias Chassis.ClusterState
  alias Chassis.KeyRoutingStrategy
  alias Chassis.ExplicitPartitionRoutingStrategy
  alias Chassis.Cluster.RequestRouter

  # FIXME
  # note, nil is a valid :key
  @type t :: %__MODULE__{}

  defstruct [
    :key,
    :key_provided,
    :partition_id,
    :state_app_name,
    :request,
    :routing_strategy,
    :context
  ]

  def new(user_request, %ClusterState{} = cluster_state, opts) when is_list(opts) do
    new(user_request, cluster_state, Enum.into(opts, %{}))
  end

  def new(user_request, %ClusterState{} = cluster_state, context) when is_map(context) do
    request = %__MODULE__{
      request: user_request,
      context: context,
      partition_id: Map.get(context, :partition_id),
      routing_strategy: Map.get(context, :routing_strategy)
    }

    context
    |> case do
      %{key: key} ->
        %__MODULE__{request | key_provided: true, key: key}

      _ ->
        %__MODULE__{request | key_provided: false}
    end
    |> case do
      %__MODULE__{routing_strategy: routing_strategy} = request when not is_nil(routing_strategy) ->
        request

      %__MODULE__{partition_id: partition_id} = request when not is_nil(partition_id) ->
        %__MODULE__{request | routing_strategy: ExplicitPartitionRoutingStrategy}

      %__MODULE__{key_provided: true} = request ->
        %__MODULE__{request | routing_strategy: KeyRoutingStrategy}

      %__MODULE__{} ->
        # TODO: better error message
        raise "if you don't provide either a :key or :partition_id, you must provide a :routing_strategy"
    end
    |> RequestRouter.route(cluster_state)
  end
end
