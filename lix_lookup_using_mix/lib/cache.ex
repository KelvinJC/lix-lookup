defmodule StaffCacheRegister do
  @moduledoc """
  `StaffCacheRegister` is responsible for generating and tracking multiple `StaffCache` agent processes. \\
  The PID for each process is stored in a list within its internal state.
  """
  def start_link(num_caches) do
    caches = create_caches(num_caches)
    [next_cache | rest_caches] = caches
    Agent.start(fn -> {next_cache, rest_caches, caches} end)
  end

  defp create_caches(num) do
    Enum.map(1..num, fn _ -> StaffCache.start_link() end)
    |> Enum.map(fn {:ok, cache_pid} -> cache_pid end)
  end

  def list_caches(agent) do
    Agent.get(agent, fn {_, _, caches} -> caches end)
  end

  @doc """
  Each query for a cache process returns the PID at the given index.
  """
  def get_cache_by_index(agent, index) do
    Agent.get(agent, fn {_, _, caches} ->
      default_cache = Enum.at(caches, 0)
      Enum.at(caches, index, default_cache)
    end)
  end

  @doc """
  Each query for a cache process returns a new PID by cycling through the list of processes.
  """
  def get_cache(agent) do
    {next_cache, _, _} = get_next_cache(agent)
    next_cache
  end

  defp get_next_cache(agent) do
    Agent.get_and_update(
      agent,
      fn {_current_cache, rest_caches, caches} = old_state ->
        new_state = loop_through_caches(rest_caches, caches)
        # Agent.get_and_update/3 only returns the first element of tuple result
        {old_state, new_state}
      end
    )
  end

  defp loop_through_caches(previous_remaining_caches, original_caches) do
    # If previous_remaining_caches is empty, reset to the original list of caches
    new_remaining_caches =
      if previous_remaining_caches == [] do
        original_caches
      else
        previous_remaining_caches
      end

    [next_cache | rest_caches] = new_remaining_caches
    {next_cache, rest_caches, original_caches}
  end
end


defmodule StaffCache do
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
