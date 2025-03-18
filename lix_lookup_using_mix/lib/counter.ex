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

  def proc_summary(success_agent, error_agent) do
    succ = get_count(success_agent)
    |> IO.inspect(label: "number of successful processes")
    err = get_count(error_agent)
    |> IO.inspect(label: "number of error processes")
    succ + err
    |> IO.inspect(label: "total number of processes")
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
