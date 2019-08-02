defmodule UniqueIdGenerator.SparseIDRanges do

  alias IntervalMap.Interval

  @type id :: UniqueIdGenerator.id()
  @type bound :: id
  @type ranges :: IntervalMap.t()


  @spec new(bound, bound) :: ranges
  def new(left, right) when left <= right do
    new()
    |> IntervalMap.put({left, right})
  end

  @spec new() :: ranges
  def new do
    IntervalMap.new
  end

  @spec pop(ranges) :: {id, ranges}
  def pop(%IntervalMap{} = ranges) do
    ranges
    |> IntervalMap.to_list()
    |> case do
         [] ->
           :error

         # interval is full
         [%Interval{left: last_id, right: max_id} = interval | _rest] when last_id + 1 == max_id ->
           id = last_id + 1
           ranges = IntervalMap.delete(ranges, interval)

           {id, ranges}

         [%Interval{left: last_id, right: max_id} = interval | _rest] ->
           id = last_id + 1

           ranges =
             ranges
             |> IntervalMap.delete(interval)
             |> IntervalMap.put({id, max_id})

           {id, ranges}
       end
  end

  @spec put(ranges, bound, bound) :: ranges
  def put(ranges, left, right) do
    IntervalMap.put(ranges, {left, right})
  end

  @spec delete(ranges, bound, bound) :: ranges
  def delete(ranges, left, right) do
    IntervalMap.delete(ranges, {left, right})
  end

  @spec contiguous?(ranges) :: boolean
  def contiguous?(ranges) when is_list(ranges) do
    ranges
    |> flatten()
    |> IntervalMap.contiguous?()
  end

  @spec range(ranges) :: {bound, bound}
  def range(ranges) when is_list(ranges) do
    %Interval{left: left, right: right} =
      ranges
      |> flatten()
      |> IntervalMap.range()

    {left, right}
  end

  defp flatten(ranges) do
    ranges
    |> Enum.flat_map(&IntervalMap.to_list/1)
    |> Enum.reduce(IntervalMap.new(), fn interval, map ->
      IntervalMap.put(map, interval)
    end)
  end
end
