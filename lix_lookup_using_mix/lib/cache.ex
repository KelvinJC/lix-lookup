defmodule StaffCacheRegister do
  use GenServer

  @moduledoc """
  `StaffCacheRegister` is responsible for generating and tracking multiple `StaffCache` agent processes. \\
  The PID for each process is stored in a list within its internal state.
  """

  ## GenServer client API
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def create(server, num_caches) do
    GenServer.call(server, {:create, num_caches})
  end

  def add(server, num_caches) do
    GenServer.cast(server, {:add, num_caches})
  end

  def list(server) do
    GenServer.call(server, {:list})
  end

  @doc """
  Each query for a cache process returns the PID at the given index.
  """
  def get_cache_by_index(server, index) do
    GenServer.call(server, {:get_cache_by_index, index})
  end

  ## GenServer callbacks
  @impl true
  def init(:ok) do
    caches = []
    refs = %{}
    {:ok, {caches, refs}}
  end

  @impl true
  def handle_cast({:add, num_caches}, state) do
    {caches, refs} = state
    case caches do
      [] ->
        {:noreply, state}

      _ ->
        pids_of_new_caches =
          for _ <- 0..num_caches - 1 do
            {:ok, pid} = StaffCache.start_link()
            pid
          end

        new_caches = pids_of_new_caches ++ caches
        {:noreply, {new_caches, refs}}
    end
  end

  @impl true
  def handle_call({:create, num_caches}, _from, _state) do
    pids_of_caches =
      for _ <- 1..num_caches do
        {:ok, cache} = StaffCache.start_link()
        cache
      end

    refs =
      for cache <- pids_of_caches,
        into: %{} do
          ref = Process.monitor(cache)
          {ref, cache}
      end

    {:reply, pids_of_caches, {pids_of_caches, refs}}
  end

  @impl true
  def handle_call({:list}, _from, state) do
    {caches, _} = state
    {:reply, caches, state}
  end

  @impl true
  def handle_call({:get_cache_by_index, index}, _from, {caches, _} = state) do
    default_cache = Enum.at(caches, 0)
    cache_pid = Enum.at(caches, index, default_cache)
    {:reply, cache_pid, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, {caches, refs}) do
    {cache, refs} = Map.pop(refs, ref)
    caches = List.delete(caches, cache)
    {:noreply, {caches, refs}}
  end

  @impl true
  def handle_info(msg, state) do
    require Logger
    Logger.debug("Unexpected message in StaffCacheRegister: #{inspect(msg)}")
    {:noreply, state}
  end
end


defmodule StaffCache do
  use Agent

  def start_link() do
    Agent.start_link(fn -> {%{}, []} end)
  end

  def get_state(agent) do
    Agent.get(agent, fn {all_staff, matched_staff} -> {all_staff, matched_staff} end)
  end

  def get_all_staff(agent) do
    Agent.get(agent, fn {all_staff, _matched_staff} -> all_staff end)
  end

  def get_all_matched_staff(agent) do
    Agent.get(agent, fn {_all_staff, matched_staff} ->
      matched_staff
    end)
  end

  def add_staff(agent, staff) when is_map(staff) do
    Agent.update(agent, fn {all_staff, matched_staff} ->
      {Map.merge(staff, all_staff), matched_staff}
    end)
  end

  def match_staff(agent, staff) do
    Agent.update(agent, fn {all_staff, matched_staff} ->
      new_matched_staff = match_staff_id_to_emails(staff, all_staff)
      {all_staff, matched_staff ++ new_matched_staff}
    end)
  end

  defp match_staff_id_to_emails(staff, all_staff) do
    staff
    |> Enum.map(fn [_, id, name, _] ->
      email = Map.get(all_staff, id)

      if email == nil do
        {:error, "Staff ID does not match any record."}
      else
        {:ok, "#{id}, #{String.trim(name)}, #{email}\n"}
      end
    end)
    |> Enum.map(fn {tag, row} ->
      case tag do
        :ok -> row
        :error -> []
      end
    end)
  end
end
