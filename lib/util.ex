defmodule Chassis.Util do
  @moduledoc false

  def parallel_each(enum, func, timeout \\ 20_000) do
    enum
    |> Enum.map(&Task.async(fn -> func.(&1) end))
    |> Enum.each(&Task.await(&1, timeout))
  end
end
