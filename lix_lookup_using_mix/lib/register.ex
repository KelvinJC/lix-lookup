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
          for num <- 0..num_caches - 1 do
            {:ok, pid} = StaffCache.start_link(name: String.to_atom("Agent#{num}"))
            pid
          end

        new_caches = pids_of_new_caches ++ caches
        {:noreply, {new_caches, refs}}
    end
  end

  @impl true
  def handle_call({:create, num_caches}, _from, _state) do
    names = [:agent01, :agent02, :agent03, :agentd, :agente, :agentf, :agentg, :agenth, :agenti, :agentj]
    pids_of_caches =
      for i <- 0..num_caches - 1 do
        {:ok, cache} = StaffCache.start_link(name: Enum.at(names, i))
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
