defmodule ArgoTest do
  use ExUnit.Case
  use ExUnitProperties

  property "cluster still serves requests while a majority of nodes are alive" do
    check all(
            cluster_size <- StreamData.member_of(3..15//2),
            servers_to_disable <-
              StreamData.integer(Range.new(0, Integer.floor_div(cluster_size, 2)))
          ) do
      unique_registry_name = System.unique_integer() |> Integer.to_string() |> String.to_atom()
      start_supervised!({Registry, keys: :duplicate, name: unique_registry_name})

      cluster_config = %{
        cluster_size: cluster_size,
        registry: unique_registry_name
      }

      # start unsupervised servers to fill cluster
      for _server <- 1..cluster_size do
        GenServer.start(
          Argo.Server,
          cluster_size: cluster_config.cluster_size,
          registry: cluster_config.registry
        )
      end

      client =
        start_supervised!({Argo.Client, registry: cluster_config.registry},
          id: System.unique_integer()
        )

      assert :ok == Argo.Client.add_command(client, :test)

      # kill off a random minority of the cluster
      cluster_config.registry
      |> Registry.lookup(:server)
      |> Enum.take_random(servers_to_disable)
      |> Enum.each(fn {pid, _val} ->
        Process.exit(pid, :kill)
      end)

      assert :ok == Argo.Client.add_command(client, :test_2)

      {:ok, log} = Argo.Client.read_log(client)

      log_vals_except_noops =
        log
        |> Enum.filter(fn {_index, val} -> val != :no_op end)
        |> Enum.map(fn {_index, val} -> val end)

      assert :test in log_vals_except_noops
      assert :test_2 in log_vals_except_noops
    end
  end
end
