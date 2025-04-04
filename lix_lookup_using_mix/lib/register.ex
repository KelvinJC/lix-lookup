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
        current_cache_count = length(caches)
        final_cache_count = current_cache_count + num_caches
        pids_of_new_caches =
          for i <- current_cache_count..final_cache_count do
            {:ok, cache} = DynamicSupervisor.start_child(CacheSupervisor, {StaffCache, String.to_atom("Cache_#{i}")})
            cache
          end

        new_caches = pids_of_new_caches ++ caches
        {:noreply, {new_caches, refs}}
    end
  end

  defp monitor_caches(pids_of_caches) do
    for pid <- pids_of_caches,
      into: %{} do
        ref = Process.monitor(pid)
        {ref, pid}
    end
  end

  @impl true
  def handle_call({:create, num_caches}, _from, _state) do
    pids_of_caches =
      for i <- 0..num_caches  - 1 do
        {:ok, cache} = DynamicSupervisor.start_child(CacheSupervisor, {StaffCache, String.to_atom("Cache_#{i}")})
        cache
      end

    refs = monitor_caches(pids_of_caches)

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
    # cache_pid = Enum.at(caches, index, default_cache)
    cache_pid = Enum.at(caches, index)
    {:reply, cache_pid, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, {_caches, _refs}) do
    # {_, refs} = Map.pop(refs, ref)
    # caches = List.delete(caches, cache)

    caches =
      for {_, pid, _, _} <- DynamicSupervisor.which_children(CacheSupervisor) do
        pid
      end

    refs = monitor_caches(caches)

    {:noreply, {caches, refs}}
  end

  @impl true
  def handle_info(msg, state) do
    require Logger
    Logger.debug("Unexpected message in StaffCacheRegister: #{inspect(msg)}")
    {:noreply, state}
  end
end
