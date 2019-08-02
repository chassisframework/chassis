defmodule Chassis.RoutingStrategy do
  @moduledoc """
  FIXME: Routing Strategy
  """

  alias Chassis.ClusterState
  alias Chassis.Reply
  alias Chassis.Request

  @type cluster_state :: ClusterState.t()
  @type request :: Request.t()
  @type partition_id :: Chassis.partition_id()
  @type strategy :: module()
  @type reply :: Reply.t()

  # @callback init(cluster_state) :: :ok
  # @callback route(cluster_state, request) :: partition_id
  # # on_reply/2 is called async of request/reply cycle
  # @callback on_reply(cluster_state, reply) :: :ok
end
