defmodule Argo do
  @moduledoc false
  use Supervisor

  @type cluster_config :: [
          supervisor_name: atom(),
          server_supervisor_name: atom(),
          registry_name: atom(),
          cluster_size: integer()
        ]

  def start_link(options) do
    options =
      options
      |> Keyword.put_new(:supervisor_name, Argo.Supervisor)
      |> Keyword.put_new(:server_supervisor_name, Argo.ServerSupervisor)
      |> Keyword.put_new(:registry_name, Argo.Registry)

    name = Keyword.get(options, :name, __MODULE__)

    Supervisor.start_link(__MODULE__, options, name: name)
  end

  @impl true
  def init(options) do
    children = [
      {Registry, keys: :duplicate, name: options[:registry_name]},
      {Argo.ServerSupervisor,
       name: options[:server_supervisor_name], cluster_size: 5, registry: options[:registry_name]}
    ]

    Supervisor.init(children, strategy: :one_for_one, name: options[:supervisor_name])
  end
end
