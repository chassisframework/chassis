# TODO: mark :cluster_state as private

defmodule Chassis.Range do
  @moduledoc """
    Ranges are "left open, right closed", meaning that the lower bound is exclusive, and the
    upper bound is inclusive.

    You can determine if a key is in a range by calling Chassis.key_in_range?/2
  """

  defstruct [:left, :right]

  @type t :: %__MODULE__{}

  @doc false
  def new({left, right}) do
    %__MODULE__{left: left, right: right}
  end
end
