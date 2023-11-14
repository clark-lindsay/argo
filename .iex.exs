:ok = Application.ensure_started(:argo)

{:ok, client} = Argo.Client.start_link(registry: Argo.Registry)
