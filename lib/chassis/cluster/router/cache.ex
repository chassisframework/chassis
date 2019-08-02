defmodule Chassis.Cluster.Router.Cache do
  @moduledoc false

  def get(cluster_name) do
    try do
      state =
        cluster_name
        |> name()
        |> :persistent_term.get()

      {:ok, state}
    rescue
      ArgumentError ->
        {:error, :not_found}
    end
  end

  def put(cluster_name, state) do
    cluster_name
    |> name()
    |> :persistent_term.put(state)
  end

  defp name(cluster_name) do
    {__MODULE__, cluster_name}
  end
end
