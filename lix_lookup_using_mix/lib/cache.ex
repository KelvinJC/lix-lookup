defmodule StaffCacheRegister do
  use GenServer

  @moduledoc """
  `StaffCacheRegister` is responsible for generating and tracking multiple `StaffCache` agent processes. \\
  The PID for each process is stored in a list within its internal state.
  """

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def create(server, num_caches) do
    GenServer.cast(server, {:create, num_caches})
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

  ## define GenServer callbacks
  @impl true
  def init(:ok) do
    {:ok, []}
  end

  @impl true
  def handle_cast({:create, num_caches}, _caches) do
    pids_of_caches =
      for _ <- 1..num_caches do
        {:ok, pid} = StaffCache.start_link()
        pid
      end
    {:noreply, pids_of_caches}
  end

  @impl true
  def handle_cast({:add, num_caches}, caches) do
    case caches do
      [] ->
        {:noreply, caches}
      _ ->
        pids_of_new_caches =
          for _ <- 1..num_caches do
            {:ok, pid} = StaffCache.start_link()
            pid
          end
        {:noreply, pids_of_new_caches ++ caches}
    end
  end

  @impl true
  def handle_call({:list}, _from, caches) do
    {:reply, caches, caches}
  end

  @impl true
  def handle_call({:get_cache_by_index, index}, _from, caches) do
    default_cache = Enum.at(caches, 0)
    cache_pid = Enum.at(caches, index, default_cache)
    {:reply, cache_pid, caches}
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
