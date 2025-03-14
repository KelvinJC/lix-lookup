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
