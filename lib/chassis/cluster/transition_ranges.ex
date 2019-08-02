# this may belong in PartitionedClusterLayout instead of Chassis proper

defmodule Chassis.Cluster.TransitionRanges do
  @moduledoc false

  alias Chassis.Cluster.RangeSteal
  alias Chassis.Cluster.RangeAccept
  alias Chassis.Cluster.RangeRelease
  alias Chassis.Range

  defstruct [
    {:active, IntervalMap.new()},
    :releasing,
    :stealing,
    {:released, IntervalMap.new()},
    {:to_steal, []}
  ]

  # this is a partition being asked to release ranges
  def new(%Range{left: left, right: right}) do
    active = IntervalMap.new() |> IntervalMap.put({left, right})

    %__MODULE__{active: active}
  end

  # this is a partition split from other partitions, and hence needs to steal ranges from them
  def new(%{} = ranges_to_steal) do
    to_steal =
      ranges_to_steal
      |> Enum.map(fn {partition_id, {left, right}} ->
        %RangeSteal{
          range: %Range{left: left, right: right},
          from_partition_id: partition_id
        }
      end)
      |> Enum.sort_by(fn %RangeSteal{range: %Range{left: left}} -> left end)

    %__MODULE__{to_steal: to_steal}
  end

  def steal_next_range(%__MODULE__{to_steal: []} = ranges) do
    {%__MODULE__{ranges | stealing: nil}, nil}
  end

  def steal_next_range(%__MODULE__{to_steal: [range_steal | rest]} = ranges) do
    {%__MODULE__{ranges | stealing: range_steal, to_steal: rest}, range_steal}
  end

  def release_range(
        %__MODULE__{active: active, released: released} = ranges,
        %RangeRelease{range: %Range{left: left, right: right}} = range_release
      ) do
    %__MODULE__{ranges | active: IntervalMap.delete(active, {left, right}), released: IntervalMap.put(released, {left, right}, range_release), releasing: range_release}
  end

  def accept_range(
        %__MODULE__{
          active: active,
          stealing: %RangeSteal{
            range: range,
            from_partition_id: from_partition_id
          }
        } = ranges,
        %RangeAccept{
          range: %Range{left: left, right: right} = range,
          from_partition_id: from_partition_id
        }
      ) do
    %__MODULE__{ranges | active: IntervalMap.put(active, {left, right}), stealing: nil}
  end

  def range_accepted(%__MODULE__{releasing: %RangeRelease{range: range}} = ranges, range) do
    %__MODULE__{ranges | releasing: nil}
  end

  def should_handle_or_forward(
        %__MODULE__{
          releasing: %RangeRelease{range: %Range{left: releasing_left, right: releasing_right}}
        } = ranges,
        digested_key
      ) do
    if releasing_left < digested_key && digested_key <= releasing_right do
      {:error, :releasing_range}
    else
      should_handle_or_forward(%__MODULE__{ranges | releasing: nil}, digested_key)
    end
  end

  def should_handle_or_forward(
        %__MODULE__{
          active: active,
          released: released
        },
        digested_key
      ) do
    if IntervalMap.key_member?(active, digested_key) do
      :handle
    else
      released
      |> IntervalMap.get_value(digested_key)
      |> case do
        {:value, %RangeRelease{to_partition_id: to_partition_id, to_nodes: to_nodes}} ->
          {:forward, to_partition_id, to_nodes}

        :not_found ->
          {:error, :not_responsible_for_key}
      end
    end
  end

  def should_steal?(%__MODULE__{to_steal: []}), do: false
  def should_steal?(%__MODULE__{to_steal: to_steal}) when is_list(to_steal), do: true
end
