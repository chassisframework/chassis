defmodule Chassis.Util.Visualization do
  @moduledoc false

  @hash_range :math.pow(2, 32) |> trunc()

  def deterministic_color(term) do
    r = deterministic_integer_for_colors({term, :red})
    g = deterministic_integer_for_colors({term, :green})
    b = deterministic_integer_for_colors({term, :blue})

    IO.ANSI.color(r, g, b)
  end

  def center(string, char \\ "=") do
    side_length = ((iex_columns() - String.length(string)) / 2) |> round()

    side = String.duplicate(char, side_length - 1)

    side <> string <> side
  end

  @doc false
  def deterministic_integer_for_colors(term) do
    trunc(5 * hash(term) / @hash_range)
  end

  @doc false
  def iex_columns do
    {:ok, columns} = :io.columns()
    columns
  end

  def color(string, color) do
    sequence = :erlang.apply(IO.ANSI, color, [])

    sequence <> string <> IO.ANSI.reset()
  end

  defp hash(term) do
    :erlang.phash2(term, @hash_range)
  end
end
