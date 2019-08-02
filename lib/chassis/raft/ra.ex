defmodule Chassis.Raft.Ra do
  @moduledoc false

  def start_group(name, machine_module, machine_args, nodes) when is_list(nodes) do
    members = Enum.map(nodes, &server_id(name, &1))
    machine = ra_machine(machine_module, %{app_args: machine_args})

    {:ok, _nodes, []} = :ra.start_cluster(name, machine, members)

    :ok
  end

  def destroy_group(name, nodes) when is_list(nodes) do
    members = Enum.map(nodes, &server_id(name, &1))

    {:ok, _} = :ra.delete_cluster(members)

    :ok
  end

  def add_member(name, nodes, node, module, init_args) when is_list(nodes) and is_atom(node) do
    members = Enum.map(nodes, &server_id(name, &1))
    new_member = server_id(name, node)

    {:ok, _, _} = :ra.add_member(members, new_member)
    :ok = :ra.start_server(name, new_member, {:module, module, init_args}, nodes)

    :ok
  end

  def known_groups do
    :ra_directory.list_registered()
    |> Enum.map(fn {name, _uid} -> name end)
  end

  def restart_local_member(name) when is_atom(name) do
    name
    |> server_id()
    |> :ra.restart_server()
  end

  def members(name, node) do
    name
    |> server_id(node)
    |> :ra.members()
    |> case do
      {:ok, members, {_name, leader}} ->
        {:ok, {leader, Keyword.values(members)}}

      error ->
        error
    end
  end

  def leader(name) do
    case :ra_leaderboard.lookup_leader(name) do
      {^name, leader} ->
        {:ok, leader}

      :undefined ->
        {:error, :not_found}
    end
  end

  def command(name, node, command) do
    name
    |> server_id(node)
    |> :ra.process_command(command)
    |> case do
      {:ok, result, {^name, _leader}} ->
        {:ok, result}

      error ->
        error
    end
  end

  def leader_query(name, node, query) do
    name
    |> server_id(node)
    |> :ra.leader_query(query)
    |> case do
      {:ok, {_index, result}, {^name, _leader}} ->
        {:ok, result}

      error ->
        error
    end
  end

  def local_query(name, node, query) do
    {:ok, {_index, result}, {^name, _leader}} =
      name
      |> server_id(node)
      |> :ra.local_query(query)

    {:ok, result}
  end

  def fn_call_effect(m, f, a) do
    {:mod_call, m, f, a}
  end

  # def consistent_query(name, node, query) do
  #   name
  #   |> server_id(node)
  #   |> :ra.consistent_query(query)
  # end

  defp server_id(name, node \\ node()) when is_atom(name) and is_atom(node) do
    {name, node}
  end

  defp ra_machine(machine_module, args) do
    {:module, machine_module, args}
  end
end
