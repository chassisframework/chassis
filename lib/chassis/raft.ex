defmodule Chassis.Raft do
  @moduledoc false

  require Logger

  alias Chassis.Raft.Ra
  alias Chassis.Cluster.Application.ClusterTaskSupervisor

  @chassis_prefix "$Chassis$"
  @joiner "."

  def start_group(group_id, machine_module, machine_args, nodes) do
    group_id
    |> encode_ra_group_name()
    |> Ra.start_group(machine_module, machine_args, nodes)
  end

  def destroy_group(group_id, nodes) do
    group_id
    |> encode_ra_group_name()
    |> Ra.destroy_group(nodes)
  end

  def add_member(group_id, node, module, init_args, nodes) do
    group_id
    |> encode_ra_group_name()
    |> Ra.add_member(nodes, node, module, init_args)
  end

  def known_group_ids do
    Ra.known_groups()
    |> Enum.filter(fn group ->
      group
      |> Atom.to_string()
      |> String.starts_with?(@chassis_prefix)
    end)
    |> Enum.map(&Atom.to_string/1)
    |> Enum.map(&decode_ra_group_name/1)
  end

  def restart_local_member(group_id) do
    group_id
    |> encode_ra_group_name()
    |> Ra.restart_member()
  end

  def async_effect(cluster_state, m, f, a) do
    Ra.fn_call_effect(
      ClusterTaskSupervisor,
      :async,
      [cluster_state, m, f, a]
    )
  end

  # defdelegate consistent_query(name, node, query), to: Ra

  def command(name, [], command) do
    Logger.error("no nodes given for command #{inspect(command)} in group #{inspect(name)}")
  end

  def command(group_id, nodes, command) do
    (&Ra.command(&1, &2, command))
    |> try_leader_first(group_id, nodes)
  end

  def leader_query(group_id, nodes, query) do
    (&Ra.leader_query(&1, &2, query))
    |> try_leader_first(group_id, nodes)
  end

  def local_query(group_id, nodes, query) do
    (&Ra.local_query(&1, &2, query))
    |> try_leader_first(group_id, nodes)
  end

  def members(group_id, nodes) do
    (&Ra.members(&1, &2))
    |> try_leader_first(group_id, nodes)
  end

  def leader(group_id, nodes) do
    group_id
    |> encode_ra_group_name()
    |> Ra.leader()
    |> case do
      {:ok, leader} ->
        {:ok, leader}

      {:error, :not_found} ->
        # this will cause Ra to cache the leader
        case members(group_id, nodes) do
          {:ok, {leader, _members}} ->
            {:ok, leader}

          error ->
            error
        end
    end
  end

  def try_leader_first(func, group_id, nodes) do
    encoded_name = encode_ra_group_name(group_id)

    encoded_name
    |> cached_leader_first(nodes)
    |> try_for_nodes(encoded_name, func)
  end

  defp cached_leader_first(encoded_name, nodes) do
    case Ra.leader(encoded_name) do
      {:ok, leader} ->
        [leader | nodes] |> Enum.uniq()

      {:error, :not_found} ->
        nodes
    end
  end

  defp try_for_nodes([], _encoded_name, _func) do
    {:error, :all_errored}
  end

  defp try_for_nodes([node | nodes], encoded_name, func) do
    encoded_name
    |> func.(node)
    |> case do
      {:ok, result} ->
        {:ok, result}

      error ->
        Logger.warn(inspect(error))
        try_for_nodes(nodes, encoded_name, func)
    end
  end

  #
  # we do this to hide our group ids in ra's string representation of raft group names
  # while maintaining inspectability with :ra.overview/0
  #
  # this isn't ideal, but it'll do for now, maybe turn into an mnesia persisted table later
  #
  defp encode_ra_group_name(term) do
    encoded_term =
      term
      |> :erlang.term_to_binary()
      |> Base.encode64()

    [@chassis_prefix, encoded_term, inspect(term)]
    |> Enum.join(@joiner)
    |> String.to_atom()
  end

  defp decode_ra_group_name(encoded_ra_group_name) do
    [@chassis_prefix, term, _human_name] = String.split(encoded_ra_group_name, @joiner, parts: 3)

    term
    |> Base.decode64!()
    |> :erlang.binary_to_term()
  end
end
