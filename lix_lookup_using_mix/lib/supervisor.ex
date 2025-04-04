defmodule CacheRegisterSupervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, :ok, opts)
  end

  @impl true
  def init(:ok) do
    children = [
      {DynamicSupervisor, name: CacheSupervisor, strategy: :one_for_one}, # :one_for_one is currently the only available strategy for dynamic supervisors.
      {StaffCacheRegister, name: CacheRegister}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end

## -- Dynamic supervisor can be defined separately
# defmodule CacheSupervisor do
#   use DynamicSupervisor

#   def start_link(opts) do
#     DynamicSupervisor.start_link(__MODULE__, :ok, opts)
#   end

#   @impl true
#   def init(:ok) do
#     DynamicSupervisor.init(strategy: :one_for_one)
#   end
# end
