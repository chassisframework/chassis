defmodule KV.State do
  alias Chassis.ClusterState

  require Logger

  # use Chassis, :state, mode: :consistent

  def init([]) do
    {:ok, Map.new()}
  end

  def handle_command({:put, v}, %{key: k} = _context, map) do
    Logger.info "KV State Replica putting #{inspect v} into #{inspect k}"

    {:reply, :ok, Map.put(map, k,  v)}
  end

  def handle_command(:delete, %{key: k} = _context, map) do
    Logger.info "KV State Replica deleting #{inspect k}"

    {:reply, :ok, Map.delete(map, k)}
  end

  def handle_query(:get, %{key: k} = _context, map) do
    reply =
      case Map.fetch(map, k) do
        {:ok, value} ->
          {:ok, value}

        :error ->
          {:error, :not_found}
      end

    {:reply, reply}
  end

  def release_range(range, cluster_state, map) do
    keys_to_release =
      map
      |> Map.keys()
      |> Enum.filter(&ClusterState.key_in_range?(cluster_state, range, &1))

    {map_to_release, map_to_keep} = Map.split(map, keys_to_release)

    {:ok, map_to_release, map_to_keep}
  end

  def accept_range(_range, incoming_map, _cluster_state, map) do
    {:ok, Map.merge(map, incoming_map)}
  end
end
