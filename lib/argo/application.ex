defmodule Argo.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications

  @moduledoc """
  TODO(doc): needs docs!
  """

  use Application

  @impl true
  @doc """
  TODO(doc): needs docs!
  """
  def start(_type, args \\ []) do
    name = Keyword.get(args, :name)

    children = [
      # Starts a worker by calling: Argo.Worker.start_link(arg)
      # {Argo.Worker, arg}
      {Registry, keys: :duplicate, name: Argo.Registry},
      {Argo.ServerSupervisor, cluster_size: 5, registry: Argo.Registry}
      # 5 raft servers
    ]

    opts = [strategy: :one_for_one, name: name]

    with {:ok, sup_pid} <- Supervisor.start_link(children, opts) do
      # TODO: return list of raft server pids
      {:ok, sup_pid, []}
    end
  end
end
