defmodule App do
  use Application

  @impl true
  def start(_type, _args) do
    # CacheSupervisor.start_link(name: CacheSupervisor)
    CacheRegisterSupervisor.start_link(name: CacheRegisterSupervisor)
  end
end
