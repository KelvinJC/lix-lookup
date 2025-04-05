defmodule Cache do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
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
  def init(:ok) do
    :ets.new(:all_staff, [
      :set,
      :public,
      :named_table,
      {:read_concurrency, true},
      {:write_concurrency, true}
    ])

    {:ok, []}
  end
end
