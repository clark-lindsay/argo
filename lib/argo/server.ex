defmodule Argo.Server do
  # TOOD(doc): fill the remainder in
  @moduledoc """
  The module that implements the behavior of an individual Raft "server", as defined in the paper.

  These servers can operate under one of 4 behaviors: [leader, candidate, follower, listener]

  ## leader
  ## candidate
  ## follower
  ## listener
  """
  require Integer
  use GenServer

  # TODO(implementation): "servers retry RPCS if they do not receive a response in a timely manner" (5.1)
  # ^ i think i would only want to retry RPCs under certain conditions... e.g. if i was trying to send an "AppendEntries" call
  # to a follower and they never responde, but i find out that i am no longer the leader, then i would want to stop retrying, right?
  # ^ could handle this by pattern matching on the current behavior of the server in the function head for a retry that is triggered
  # by a scheduled message-to-self, similar to how i am handling the election timeouts

  # TODO(test): "if a server receives a request with a stale term number, it rejects the request" (5.1)

  # TODO(implementation): client API for adding entries to the leader's log, which will then be replicated to the cluster with "AppendEntries" rounds
  # ^ requests that hit a follower should be redirected to leader? or should they simply reply with the leader's address?
  # TODO(implementation): remove periodic "AppendEntries" heartbeats (should the periodic timer be reset if the leader has sent out real entries?)

  defmodule RequestVote do
    @moduledoc false
    defstruct([:term, :candidate_id, :last_log_index, :last_log_term])
  end

  defmodule RequestVoteReply do
    @moduledoc false
    defstruct([:sender, :term, :vote_granted])
  end

  defmodule AppendEntries do
    @moduledoc false
    defstruct([:term, :leader_id, :prev_log_index, :prev_log_term, :entries, :leader_commit_index])
  end

  defmodule AppendEntriesReply do
    @moduledoc false
    defstruct([:sender, :term, :success])
  end

  def start_link(cluster_config) do
    GenServer.start_link(__MODULE__, cluster_config)
  end

  ## Callbacks

  @impl true
  def init(cluster_config) do
    if Integer.is_odd(cluster_config[:cluster_size]) and cluster_config[:cluster_size] >= 3 do
      registry = Keyword.fetch!(cluster_config, :registry)
      # registered without a unique key since all servers are treated the same,
      # as they could be following any behavior at any time
      Registry.register(registry, :server, nil)

      # most of the state values come from O&O Figure 2
      # additional values have been added for book-keeping or convenience
      state = %{
        behavior: :follower,
        cluster_config: cluster_config,
        # TODO(implementation): current_term and voted_for should be coupled in state
        last_known_leader: nil,
        current_term: 0,
        voted_for: nil,
        # {index, term, value}
        log: [{0, 0, nil}],
        commit_index: 0,
        last_applied: 0,
        # ----- Leader state
        # server_id => next log index to send (int)
        next_index: %{},
        # server_id => highest known replicated log index (int)
        match_index: %{},
        # TODO(design): is this the best way to capture votes?
        # maps term_number => [ids_that_voted_for_me]
        votes: %{},
        # usually used to inform the client that a command has been sufficiently replicated
        # {index, term} => callback from client
        client_callbacks: %{}
      }

      # the timeout ref should be changed, and the scheduled timeout message
      # sent again, whenever an "AppendEntries" RPC is received from a current leader
      # or when granting a vote to a candidate in response to a "RequestVote" RPC
      state = refresh_election_timout(state)

      {:ok, state}
    else
      {:stop, {:bad_cluster_size, cluster_config[:cluster_size]}}
    end
  end

  # ----- Client API -----
  # See O&O Section 8

  # TODO(implementation & design): should be async, b/c the leader can only respond
  # when the entry has been replicated to a majority of the cluster
  # ^ would be easier to test, since i could do `assert_receive`
  # TODO(consider, implementation): should i add a timeout/ deadline for the response?
  # use a call so that the client knows that it reached the cluster, and gets
  # info about the last known leader
  # then the callback is triggered when the command is replicated to a majority of the cluster
  def add_command(server_pid, {command, request_serial_number}, _callback \\ fn -> :ok end) do
    try do
      GenServer.call(server_pid, {:add_command, command, request_serial_number, self()})
    rescue
      error ->
        {:error, error}
    catch
      :exit, value ->
        {:error, value}

      thrown_value ->
        {:error, thrown_value}
    end
  end

  # ----- Callback API -----

  @impl GenServer
  def handle_call(
        {:add_command, command, req_serial_num, client_pid},
        _from,
        %{behavior: :leader} = state
      ) do
    # The log will be replicated when the next heartbeat is sent out
    {last_index, _, _} = hd(state.log)
    new_index = last_index + 1
    reply_callback = fn -> send(client_pid, {:add_command_success, req_serial_num}) end

    state =
      state
      |> Map.update!(:log, fn log -> [{new_index, state.current_term, command} | log] end)
      |> put_in([:client_callbacks, {new_index, state.current_term}], reply_callback)

    {:reply, {:ok, self()}, state}
  end

  @impl GenServer
  def handle_call(
        {:add_command, _command, _req_serial_num, _client_pid} = msg,
        _from,
        %{behavior: behavior} = state
      )
      when behavior != :leader do
    if not is_nil(state.last_known_leader) do
      reply =
        try do
          GenServer.call(state.last_known_leader, msg)
        rescue
          error ->
            {:error, error}
        catch
          :exit, value ->
            {:error, value}

          thrown_value ->
            {:error, thrown_value}
        end

      {:reply, reply, state}
    else
      {:reply, {:error, :unknown_leader}, state}
    end
  end

  @impl GenServer
  def handle_info(:heartbeat, %{behavior: :leader} = state) do
    send_heartbeat(state)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:heartbeat, %{behavior: behavior} = state) when behavior != :leader do
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:election_timeout, _ref}, %{behavior: :leader} = state) do
    # Handle the reflexive, delayed message that indicates an election timeout.

    # Outlined in O&O Figure 2
    # Specified in O&O Section 5.2
    {:noreply, refresh_election_timout(state)}
  end

  @impl GenServer
  def handle_info({:election_timeout, ref}, state) do
    if ref == state.election_timeout_ref do
      state =
        state
        |> Map.put(:behavior, :candidate)
        |> Map.put(:current_term, state.current_term + 1)
        |> Map.put(:voted_for, self())
        |> Map.put(:votes, %{(state.current_term + 1) => [self()]})
        |> refresh_election_timout()

      {last_log_index, last_log_term, _val} = hd(state.log)

      Registry.dispatch(state.cluster_config[:registry], :server, fn servers ->
        for {pid, _} <- servers, pid != self() do
          GenServer.cast(pid, %RequestVote{
            term: state.current_term,
            candidate_id: self(),
            last_log_index: last_log_index,
            last_log_term: last_log_term
          })
        end
      end)

      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info(
        %RequestVoteReply{term: reply_term} = reply,
        %{current_term: current_term} = state
      )
      when reply_term > current_term do
    # Handles the replies for the "RequestVote RPC".

    # Outlined in O&O Figure 2
    # Specified in O&O Section 5.1, 5.2, 5.4
    state =
      state
      |> refresh_election_timout()
      |> Map.put(:current_term, reply.term)
      |> Map.put(:voted_for, nil)
      |> Map.put(:behavior, :follower)

    {:noreply, state}
  end

  def handle_info(%RequestVoteReply{} = _reply, %{behavior: behavior} = state)
      when behavior != :candidate do
    {:noreply, state}
  end

  def handle_info(
        %RequestVoteReply{term: reply_term, vote_granted: true} = reply,
        %{behavior: :candidate, current_term: current_term} = state
      )
      when reply_term == current_term do
    # TODO(design): use cond instead of nested ifs?
    # not sure, since i would have to do the vote counting bit twice
    state =
      Map.update(
        state,
        :votes,
        %{reply.term => [reply.sender]},
        &Map.update(&1, reply.term, [reply.sender], fn list -> [reply.sender | list] end)
      )

    my_vote_count = Enum.count(state.votes[state.current_term])

    servers = Registry.lookup(state.cluster_config[:registry], :server)

    majority_for_cluster = Integer.floor_div(state.cluster_config[:cluster_size], 2) + 1

    state =
      if(my_vote_count >= majority_for_cluster) do
        {last_log_index, _last_log_term, _val} = hd(state.log)

        next_append_indices =
          for {pid, _} <- servers, pid != self(), into: %{} do
            {pid, last_log_index + 1}
          end

        match_indices =
          for {pid, _} <- servers, pid != self(), into: %{} do
            {pid, 0}
          end

        state =
          state
          |> Map.put(:behavior, :leader)
          |> Map.put(:last_known_leader, self())
          |> Map.put(:next_index, next_append_indices)
          |> Map.put(:match_index, match_indices)
          |> Map.update!(:log, fn log -> [{last_log_index + 1, state.current_term, nil} | log] end)
        # ^ add a no-op log entry to force sync of committed entries
        # See O&O Section 8

        send_heartbeat(state, empty?: true)

        state
      else
        state
      end

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(%AppendEntriesReply{term: reply_term}, %{behavior: behavior} = state)
      when behavior != :leader do
    state =
      state
      |> Map.put(:behavior, :follower)
      |> Map.update!(:voted_for, fn current ->
        if reply_term > state.current_term do
          nil
        else
          current
        end
      end)
      |> Map.update!(:current_term, &max(&1, reply_term))

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(
        %AppendEntriesReply{term: reply_term},
        %{current_term: current_term, behavior: :leader} = state
      )
      when reply_term > current_term do
    state =
      state
      |> refresh_election_timout()
      |> Map.put(:behavior, :follower)
      |> Map.put(:current_term, reply_term)
      |> Map.put(:voted_for, nil)

    {:noreply, state}
  end

  def handle_info(
        %AppendEntriesReply{sender: sender, success: true},
        %{behavior: :leader} = state
      ) do
    {last_log_index, _, _} = hd(state.log)

    state =
      state
      |> update_in([:next_index, sender], fn
        nil -> last_log_index + 1
        val -> min(val + 1, last_log_index + 1)
      end)
      |> then(&put_in(&1, [:match_index, sender], get_in(&1, [:next_index, sender]) - 1))

    # could perform the below as part of a scheduled cleanup, instead of on every successful reply,
    # since it requires calling out to the registry for every log acc
    servers =
      state.cluster_config[:registry]
      |> Registry.lookup(:server)
      |> Enum.map(fn {pid, _val} -> pid end)

    majority_for_cluster = Integer.floor_div(state.cluster_config[:cluster_size], 2) + 1

    replicants_by_index =
      state.match_index
      |> Enum.filter(fn {server_id, _index} -> server_id in servers end)
      |> Enum.frequencies_by(fn {_server_id, index} -> index end)

    {highest_commitable_index, remaining_callbacks} =
      for {{index, _term} = key, callback} <- state.client_callbacks,
          reduce: {state.commit_index, %{}} do
        {commit_index, remaining_callbacks} ->
          if Map.get(replicants_by_index, index, 0) + 1 >= majority_for_cluster do
            callback.()

            {max(index, commit_index), remaining_callbacks}
          else
            {commit_index, Map.put(remaining_callbacks, key, callback)}
          end
      end

    state =
      state
      |> Map.put(:client_callbacks, remaining_callbacks)
      |> Map.put(:commit_index, highest_commitable_index)

    {:noreply, state}
  end

  def handle_info(
        %AppendEntriesReply{sender: sender, success: false},
        %{behavior: :leader} = state
      ) do
    # Failure due to non-matching log entries necessitates **at least one** retry,
    # with an "induction" step, where the leader's log is walked backwards until the
    # leader and follower can find a common ancestor.

    # See O&O Section 5.3
    next_append_index = max(1, (get_in(state, [:next_index, sender]) || 1) - 1)
    state = update_in(state, [:next_index, sender], fn _val -> next_append_index end)

    [
      append_entry,
      {prev_log_index, prev_log_term, _val} | _rest
    ] =
      Enum.drop_while(state.log, fn {index, _, _} ->
        index != next_append_index
      end)

    GenServer.cast(sender, %AppendEntries{
      term: state.current_term,
      leader_id: self(),
      leader_commit_index: state.commit_index,
      prev_log_index: prev_log_index,
      prev_log_term: prev_log_term,
      entries: [append_entry]
    })

    {:noreply, state}
  end

  @impl GenServer
  def handle_cast(
        %AppendEntries{term: req_term} = append_request,
        %{current_term: current_term} = state
      )
      when req_term < current_term do
    send(append_request.leader_id, %AppendEntriesReply{
      sender: self(),
      term: state.current_term,
      success: false
    })

    {:noreply, state}
  end

  @impl GenServer
  def handle_cast(
        %AppendEntries{term: req_term} = append_request,
        %{current_term: current_term} = state
      )
      when req_term >= current_term do
    # See O&O Section 5.3, and Figure 3
    %AppendEntries{
      prev_log_index: req_prev_index,
      prev_log_term: req_prev_term,
      entries: req_entries
    } = append_request

    state =
      state
      |> refresh_election_timout()
      |> Map.put(:current_term, req_term)
      |> Map.put(:behavior, :follower)
      |> Map.put(:last_known_leader, append_request.leader_id)
      |> Map.update!(:voted_for, fn val -> if(req_term > current_term, do: nil, else: val) end)

    new_log_or_fail =
      Enum.reduce_while(state.log, false, fn {index, term, _value}, acc ->
        cond do
          index == req_prev_index and term == req_prev_term ->
            # "Log Matching property" states that all preceding entries are identical
            # See O&O Section 5.3
            {_ignored_entries, identical_entries} =
              Enum.split(state.log, Enum.count(state.log) - index - 1)

            {:halt, Enum.concat(req_entries, identical_entries)}

          index == req_prev_index and term != req_prev_term ->
            {:halt, false}

          true ->
            {:cont, acc}
        end
      end)

    case new_log_or_fail do
      new_log when is_list(new_log) ->
        commit_index =
          if append_request.leader_commit_index > state.commit_index do
            {latest_index, _, _} = hd(new_log)

            min(append_request.leader_commit_index, latest_index)
          else
            state.commit_index
          end

        state =
          state
          |> Map.put(:log, new_log)
          |> Map.put(:commit_index, commit_index)

        send(append_request.leader_id, %AppendEntriesReply{
          sender: self(),
          term: state.current_term,
          success: true
        })

        {:noreply, state}

      _ ->
        send(append_request.leader_id, %AppendEntriesReply{
          sender: self(),
          term: state.current_term,
          success: false
        })

        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_cast(%RequestVote{term: req_term} = request, %{current_term: current_term} = state)
      when req_term < current_term do
    # Handles the "RequestVote RPC".

    # Outlined in O&O Figure 2
    # Specified in O&O Section 5.1, 5.2, 5.4

    # Note: this is implemented as a `cast` which replies async instead of a `call` to avoid blocking.
    send(
      request.candidate_id,
      %RequestVoteReply{sender: self(), term: state.current_term, vote_granted: false}
    )

    {:noreply, state}
  end

  def handle_cast(%RequestVote{term: req_term} = request, %{current_term: current_term} = state)
      when req_term > current_term do
    state =
      state
      |> Map.put(:voted_for, nil)
      |> Map.put(:current_term, request.term)

    {last_log_index, last_log_term, _val} = hd(state.log)

    # TODO(implementation): check section 5.4.1 to ensure that this is correct
    is_candidate_log_up_to_date? =
      request.last_log_index >= last_log_index and request.last_log_term >= last_log_term

    if is_candidate_log_up_to_date? do
      state =
        state
        |> refresh_election_timout()
        |> Map.put(:voted_for, request.candidate_id)

      send(
        request.candidate_id,
        %RequestVoteReply{sender: self(), term: state.current_term, vote_granted: true}
      )

      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  def handle_cast(%RequestVote{term: req_term} = request, %{current_term: current_term} = state)
      when req_term == current_term do
    {last_log_index, last_log_term, _val} = hd(state.log)

    # TODO(implementation): check section 5.4.1 to ensure that this is correct
    is_candidate_log_up_to_date? =
      request.last_log_index >= last_log_index and request.last_log_term >= last_log_term

    if (is_nil(state.voted_for) or state.voted_for == request.candidate_id) and
         is_candidate_log_up_to_date? do
      state =
        state
        |> Map.put(:voted_for, request.candidate_id)
        |> refresh_election_timout()

      send(
        request.candidate_id,
        %RequestVoteReply{sender: self(), term: state.current_term, vote_granted: true}
      )

      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  @spec refresh_election_timout(map()) :: map()
  defp refresh_election_timout(state) do
    # TODO(design): does the current timeout number need to be in state?
    # TODO(optimize): could i use an adaptive rate-seeking algorithm, like AIMD,
    # to find the current timeout limit that is safe to use, given the current time
    # that a total cluster broadcast takes?
    new_election_timeout_in_ms = Enum.random(150..300)
    new_election_timeout_ref = make_ref()

    Process.send_after(
      self(),
      {:election_timeout, new_election_timeout_ref},
      new_election_timeout_in_ms
    )

    state
    |> Map.put(:election_timeout_in_ms, new_election_timeout_in_ms)
    |> Map.put(:election_timeout_ref, new_election_timeout_ref)
  end

  defp send_heartbeat(state, options \\ [empty?: false]) do
    {last_log_index, last_log_term, _val} = hd(state.log)

    :ok =
      Registry.dispatch(state.cluster_config[:registry], :server, fn servers ->
        for {pid, _} <- servers, pid != self() do
          {prev_index, prev_term, entry_to_append} =
            if options[:empty?] or last_log_index < state.next_index[pid] do
              {last_log_index, last_log_term, []}
            else
              [
                append_entry,
                {prev_log_index, prev_log_term, _val} | _rest
              ] =
                Enum.drop_while(state.log, fn {index, _, _} ->
                  index != state.next_index[pid]
                end)

              {prev_log_index, prev_log_term, [append_entry]}
            end

          GenServer.cast(pid, %AppendEntries{
            term: state.current_term,
            leader_id: self(),
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            entries: entry_to_append,
            leader_commit_index: state.commit_index
          })
        end
      end)

    Process.send_after(self(), :heartbeat, 50)
  end
end
