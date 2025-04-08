defmodule App do
  use Application

  @impl true
  def start(_type, _args) do
    AppSupervisor.start_link(name: AppSupervisor)
  end
end
