defmodule UniqueIdGenerator.State do
  use Chassis.State, mode: :consistent

  require Logger

  alias UniqueIdGenerator.SparseIDRanges
  alias Chassis.Range

  # use Chassis, :state, mode: :consistent

  def init([], %{range: %Range{left: left, right: right}}) do
    {:ok, SparseIDRanges.new(left, right)}
  end

  def init([], %{range: nil}) do
    {:ok, SparseIDRanges.new()}
  end

  def handle_command(:get, _context, ranges) do
    case SparseIDRanges.pop(ranges) do
      {id, ranges} ->
        Logger.info "Issuing ID #{id}"

        {:reply, {:ok, id}, ranges}

      :error ->
        {:reply, {:error, :partition_full}, ranges}
    end
  end

  def handle_query(:state, _context, ranges) do
    {:reply, ranges}
  end

  def release_range(%Range{left: left, right: right}, _cluster_state, ranges) do
    {:ok, nil, SparseIDRanges.delete(ranges, left, right)}
  end

  def accept_range(%Range{left: left, right: right}, nil, _cluster_state, ranges) do
    {:ok, SparseIDRanges.put(ranges, left, right)}
  end
end
