defmodule Argo.ServerSupervisor do
  use Supervisor

  def start_link(options \\ []) do
    options = Keyword.put_new(options, :name, __MODULE__)

    Supervisor.start_link(__MODULE__, options, name: options[:name])
  end

  @impl true
  def init(args) do
    cluster_size = Keyword.fetch!(args, :cluster_size)
    registry = Keyword.fetch!(args, :registry)

    Registry.register(registry, __MODULE__, args[:name])

    children =
      for server <- 1..cluster_size do
        Supervisor.child_spec({Argo.Server, args}, id: {Argo.Server, server})
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
