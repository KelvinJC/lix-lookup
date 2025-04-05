defmodule StaffCache do
  use Agent

  def start_link(name) do
    Agent.start_link(fn -> {%{}, []} end, name: name)
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
    # TODO: eliminate
    |> Enum.map(fn {tag, row} ->
      case tag do
        :ok -> row
        :error -> []
      end
    end)
  end

  def clear_cache(agent) do
    Agent.get_and_update(agent, fn {all_staff, matched_staff} ->
      {{all_staff, matched_staff}, {%{}, []}}
    end)
  end
end


defmodule Cache do
  use GenServer

  def start_link(_) do
    Genserver.init(__MODULE__, :ok)
  end

  def get(key) do
    case :ets.lookup(:all_staff, key) do
      [] ->
        nil
      [{_key, value}] ->
        value
    end
  end

  def put(key, value), do: :ets.insert(:all_staff, {key, value})

  @impl true
  def init(arg) do
    :ets.new(:all_staff, [
      :set,
      :public,
      :named_table,
      {:read_concurrency, true},
      {:write_concurrency, true}
    ])

    {:ok, arg}
  end
end
