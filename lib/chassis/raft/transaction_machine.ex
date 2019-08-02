defmodule Chassis.Raft.TransactionMachine do
  @type raft_group_id :: any()
  @type raft_group_state :: any()
  @opaque id :: {raft_group_id(), integer()}
  @opaque transactions :: %{id() => TwoPhaseCommit.t()}


  @callback raft_group_id(raft_group_state()) :: raft_group_id()
  @callback get_transactions(raft_group_state()) :: t()
  @callback put_transactions(raft_group_state(), t()) :: raft_group_state()

  def create_transaction(state, global_id_prefix, participants \\ [])

  def create_transaction(nil, global_id_prefix, participants) do
    create_transaction(%{}, global_id_prefix, participants)
  end

  def create_transaction(transactions, global_id_prefix, participants) do
    id = generate_id(transactions, global_id_prefix)
    transaction = TwoPhaseCommit.new(participants, id: id)

    {id, Map.put(transactions, id, transaction)}
  end

  #
  # transaction ids are shared across all members of the raft group, and :erlang.unique_integer()
  # only guarantees uniqueness on a single runtime system, it should be rare that we see any
  # collisions, if we do, we just generate a new id. might be worth keeping stats on.
  #
  defp generate_id(transactions, global_id_prefix) do
    id = {global_id_prefix, :erlang.unique_integer()}

    if Map.has_key?(transactions, id) do
      create_id(transactions, global_id_prefix)
    else
      id
    end
  end

  defmacro __using__(_opts) do
    quote do
      @behaviour unquote(__MODULE__)

      def apply(meta, {:transaction, {:create, participants}}, state) do
        raft_group_id = raft_group_id(state)

        {id, transactions} =
          state
          |> get_transactions()
          |> unquote(__MODULE__).create_transaction(raft_group_id, participants)

        {put_transactions(state, transactions), {:ok, id}}
      end
    end
  end
end
