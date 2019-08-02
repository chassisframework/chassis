defmodule Chassis.Util.Tree do
  def tree(supervisor) do
    supervisor
    |> do_tree(0)
    |> List.flatten()
    |> Enum.join("\n")
    |> IO.puts()
  end

  defp do_tree(supervisor, depth) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.map(fn
      {id, pid, :supervisor, module} ->
        str = String.duplicate(" ", depth) <> "|-" <> "#{inspect(pid)} #{inspect(module)} #{inspect(id)}"
        [str | do_tree(pid, depth + 1)]

      {id, pid, :worker, module} ->
        String.duplicate(" ", depth) <> "|-" <> "#{inspect(pid)} #{inspect(module)} #{inspect(id)}"
    end)
  end
end
