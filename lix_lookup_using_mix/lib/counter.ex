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

  def find_staff_email([_, id, name, _], agent) do
    Agent.get(agent, fn (state) ->
      email = Map.get(state, id)
      if email == nil do
        {:error, {:key_not_found}}
      else
        {:ok, {id, name, email}}
      end
    end)
  end

  def get_map(agent) do
    Agent.get(agent, fn (state) -> state end)
  end
end
