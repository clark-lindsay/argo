defmodule Argo.Client do
  @moduledoc false
  use GenServer

  @spec start_link(options :: [name: atom() | {:via, atom(), {atom(), term()}}, registry: atom()]) ::
          GenServer.on_start()
  def start_link(options) do
    gen_server_opts = Keyword.take(options, [:name, :timeout, :debug])

    GenServer.start_link(__MODULE__, options, gen_server_opts)
  end

  @impl GenServer
  def init(options) do
    registry = Keyword.fetch!(options, :registry)

    state = %{
      registry: registry
    }

    {:ok, state}
  end

  # ----- Client API

  @spec add_command(client :: pid(), options :: [timeout: integer(), retry_interval: integer()]) ::
          term()
  def add_command(client, command, options \\ []) do
    options =
      options
      |> Keyword.put_new(:timeout, 2_000)
      |> Keyword.put_new(:retry_interval, 200)

    GenServer.call(client, {:add_command, command, options})
  end

  # ----- Callback API

  # TODO(design, implementation): cap maximum retry amount?
  @impl GenServer
  def handle_call({:add_command, command, options}, from, state) do
    # TODO(implementation, design): do i need to store this in the client's state?
    # if so, how should i store it?
    request_serial_number = System.unique_integer([:positive, :monotonic])

    {:noreply, state,
     {:continue,
      %{
        command: command,
        from: from,
        request_serial_number: request_serial_number,
        options: options,
        timeout: options[:timeout]
      }}}
  end

  @impl GenServer
  def handle_continue(%{timeout: timeout, from: from}, state) when timeout <= 0 do
    GenServer.reply(from, {:error, :timeout})

    {:noreply, state}
  end

  @impl GenServer
  def handle_continue(
        %{
          options: options,
      timeout: timeout,
          from: from,
          command: command,
          request_serial_number: request_serial_number
        },
        state
      ) do
    # request has not reached the cluster, and must be attempted or repeated
    start_time = System.monotonic_time()

    server_pids =
      for {pid, _value} <- Registry.lookup(state.registry, :server) do
        pid
      end

    random_server =
      if state[:last_known_leader] in server_pids,
        do: state.last_known_leader,
        else: Enum.random(server_pids)

    with {:ok, leader} <-
           Argo.Server.add_command(random_server, {command, request_serial_number}) do
      state = Map.put(state, :last_known_leader, leader)

      receive do
        {:add_command_success, ^request_serial_number} ->
          GenServer.reply(from, :ok)

          {:noreply, state}
      after
        options[:retry_interval] ->
          elapsed_time_for_attempt_in_ms = elapsed_time(start_time, System.monotonic_time())

          new_timeout = max(0, timeout - elapsed_time_for_attempt_in_ms)

          {:noreply, state,
           {:continue,
            %{
              options: options,
              timeout: new_timeout,
              from: from,
              request_serial_number: request_serial_number
            }}}

          # only fails if the cluster could not find a leader, so we must wait for an election to resolve

          # TODO(design): using `Process.sleep/1` always feels wrong... but in this case, is it right?
      end
    else
      _ ->
        elapsed_time_for_attempt_in_ms = elapsed_time(start_time, System.monotonic_time())

        retry_delay = max(0, options[:retry_interval] - elapsed_time_for_attempt_in_ms)
        Process.sleep(retry_delay)

        new_timeout = max(0, timeout - elapsed_time_for_attempt_in_ms - retry_delay)

        {:noreply, state,
         {:continue,
          %{
            options: options,
            timeout: new_timeout,
            from: from,
            command: command,
            request_serial_number: request_serial_number
          }}}
    end
  end

  @impl GenServer
  def handle_continue(
        %{options: options, timeout: timeout, from: from, request_serial_number: request_serial_number},
        state
      ) do
    # request has been made to the cluster, which had a leader when the call was made,
    # but no response has been received
    start_time = System.monotonic_time()

    receive do
      {:add_command_success, ^request_serial_number} ->
        GenServer.reply(from, :ok)

        {:noreply, state}
    after
      options[:retry_interval] ->
        elapsed_time_for_attempt_in_ms = elapsed_time(start_time, System.monotonic_time())

        new_timeout = max(0, timeout - elapsed_time_for_attempt_in_ms)

        {:noreply, state,
         {:continue,
          %{
            options: options,
            timeout: new_timeout,
            from: from,
            request_serial_number: request_serial_number
          }}}
    end
  end

  defp elapsed_time(from, to, unit \\ :millisecond) do
    System.convert_time_unit(to - from, :native, unit)
  end
end
