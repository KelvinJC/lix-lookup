# defmodule StaffCacheRegister do
#   use GenServer

#   @moduledoc """
#   `StaffCacheRegister` is responsible for generating and tracking multiple `StaffCache` agent processes. \\
#   The PID for each process is stored in a list within its internal state.
#   """

#   ## GenServer client API
#   def start_link(opts \\ []) do
#     GenServer.start_link(__MODULE__, :ok, opts)
#   end

#   def create(server, num_caches) do
#     GenServer.call(server, {:create, num_caches})
#   end

#   def add(server, num_caches) do
#     GenServer.cast(server, {:add, num_caches})
#   end

#   def list(server) do
#     GenServer.call(server, {:list})
#   end

#   @doc """
#   Each query for a cache process returns the PID at the given index.
#   """
#   def get_cache_by_index(server, index) do
#     GenServer.call(server, {:get_cache_by_index, index})
#   end

#   ## GenServer callbacks
#   @impl true
#   def init(:ok) do
#     {:ok, []}
#   end

#   @impl true
#   def handle_cast({:add, num_caches}, caches) do
#     case caches do
#       [] ->
#         {:noreply, caches}
#       _ ->
#         pids_of_new_caches =
#           for _ <- 1..num_caches do
#             {:ok, pid} = StaffCache.start_link()
#             pid
#           end
#         {:noreply, pids_of_new_caches ++ caches}
#     end
#   end

#   @impl true
#   def handle_call({:create, num_caches}, _caches) do
#     pids_of_caches =
#       for _ <- 1..num_caches do
#         {:ok, pid} = StaffCache.start_link()
#         pid
#       end
#     {:reply, pids_of_caches, pids_of_caches}
#   end

#   @impl true
#   def handle_call({:list}, _from, caches) do
#     {:reply, caches, caches}
#   end

#   @impl true
#   def handle_call({:get_cache_by_index, index}, _from, caches) do
#     default_cache = Enum.at(caches, 0)
#     cache_pid = Enum.at(caches, index, default_cache)
#     {:reply, cache_pid, caches}
#   end
# end
