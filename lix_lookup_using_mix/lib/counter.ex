defmodule ProcessCounter do
  def start_link(_opts) do
    Agent.start_link(fn -> 0 end)
  end

  def get_count(agent) do
    Agent.get(agent, fn (state) -> state end)
  end

  def increment_count(agent) do
    Agent.update(agent, fn (state) -> state + 1 end)
  end
end

defmodule Staff do
  def start_link(map) do
    Agent.start_link(fn -> map end)
  end

  def find_staff_email(agent, [_, id, _, _]) do
    Agent.get(agent, fn (state) ->
      IO.inspect(id, label: "id")
      email = Map.get(state, id)
      if email == nil do
        {:error, :key_not_found}
      else
        {:ok, email}
      end
    end)
  end

  def get_map(agent) do
    Agent.get(agent, fn (state) -> state end)
  end
end
