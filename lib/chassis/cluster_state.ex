defmodule Chassis.ClusterState do
  @moduledoc """
  Cluster State

  TODO: make struct members specs private
  """

  alias Chassis.Range
  alias PartitionedClusterLayout.Node
  alias PartitionedClusterLayout.Transition
  alias Chassis.ClusterState.Config
  alias Chassis.Cluster.Membership.Changes.JoinChange

  @type state ::
          :initializing
          | {:initializing_partitions, ready_partitions :: list()}
          | :running
          | :destroying_cluster
          | :computing_new_layout
          | {:transitioning, plan :: Transition.t()}

  # TODO
  @type t :: %__MODULE__{}

  defstruct [
    :init_args,
    :config,
    :layout,
    :next_layout,
    {:state, :initializing},
    {:subscribers, []},
    {:staged_changes, []},
    :starter_pid,
    {:version, 0}
  ]

  def cluster_name(%__MODULE__{config: %Config{cluster_name: cluster_name}}) do
    cluster_name
  end

  def state_apps(%__MODULE__{config: %Config{state_apps: state_apps}}) do
    state_apps
  end

  def state_app_module(%__MODULE__{config: %Config{state_apps: state_apps}}, name) do
    {state_app_module, _init_args} = Map.get(state_apps, name)

    state_app_module
  end

  def node_names(%__MODULE__{layout: layout}) do
    layout
    |> PartitionedClusterLayout.nodes()
    |> Enum.map(fn %Node{name: name} -> name end)
  end

  def node_names(%__MODULE__{layout: layout}, partition_id) do
    layout
    |> PartitionedClusterLayout.nodes_for_partition_id(partition_id)
    |> Enum.map(fn %Node{name: name} -> name end)
  end

  def v_nodes_by_partition_id(%__MODULE__{layout: layout}) do
    PartitionedClusterLayout.v_nodes_by_partition_id(layout)
  end

  def range_for_partition_id(%__MODULE__{layout: layout}, partition_id) do
    layout
    |> PartitionedClusterLayout.ranges_by_partition_id()
    |> Map.get(partition_id)
    |> Range.new()
  end

  def partition_ids(%__MODULE__{layout: layout}) do
    PartitionedClusterLayout.partition_ids(layout)
  end

  def partition_id_for_key(%__MODULE__{layout: layout}, key) do
    PartitionedClusterLayout.partition_id_for_key(layout, key)
  end

  def partition_id_for_digested_key(%__MODULE__{layout: layout}, digested_key) do
    PartitionedClusterLayout.partition_id_for_digested_key(layout, digested_key)
  end

  def digest_key(%__MODULE__{layout: layout}, key) do
    PartitionedClusterLayout.digest_key(layout, key)
  end

  def key_in_range?(%__MODULE__{} = state, %Range{left: left, right: right}, key) do
    digested_key = digest_key(state, key)

    left < digested_key && digested_key <= right
  end

  def add_subscriber(%__MODULE__{subscribers: subscribers} = state, pid) do
    %__MODULE__{state | subscribers: Enum.uniq([pid | subscribers])}
  end

  def remove_subscriber(%__MODULE__{subscribers: subscribers} = state, pid) do
    %__MODULE__{state | subscribers: Enum.reject(subscribers, fn s -> s == pid end)}
  end

  def inform_subscribers(%__MODULE__{subscribers: subscribers, config: %Config{cluster_name: cluster_name}}, who, event) do
    Enum.each(subscribers, fn subscriber ->
      Process.send(subscriber, {:chassis_event, cluster_name, who, event}, [:noconnect, :nosuspend])
    end)
  end

  # def add_nodes(%__MODULE__{config: config, layout: layout}, nodes_and_args) do
  #   Layout.add_nodes(layout, config, nodes_and_args)
  # end

  # TODO: pass join args through to cluster strategy
  def compute_new_layout_and_plan(%__MODULE__{staged_changes: staged_changes, layout: layout}) do
    nodes = Enum.map(staged_changes, fn %JoinChange{node: node, args: _args} -> node end)

    next_layout = PartitionedClusterLayout.add_nodes(layout, nodes: nodes)
    plan = PartitionedClusterLayout.transition(layout, next_layout)

    {next_layout, plan}
  end

  def promote_next_layout(%__MODULE__{next_layout: next_layout, version: version} = state) do
    %__MODULE__{state | layout: next_layout, next_layout: nil, version: version + 1}
  end

  def version(%__MODULE__{version: version}), do: version

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(_state, _opts) do
      concat(["#Chassis.Cluster.State<", "fixme", ">"])
    end
  end
end
