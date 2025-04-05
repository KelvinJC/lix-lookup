defmodule Cache do
  use GenServer

  def start_link(_) do
    Genserver.init(__MODULE__, :ok)
  end

  def get(staff_id) do
    case :ets.lookup(:all_staff, staff_id) do
      [] ->
        nil
      [{_staff_id, value}] ->
        value
    end
  end

  # TODO: move this business logic to process making lookup
  def match_staff_id_to_emails(staff) do
    staff
    |> Enum.map(fn [_, id, name, _] ->
      email = get(id)

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
