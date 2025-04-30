defmodule StaffCacheRegisterTest do
  use ExUnit.Case, async: true

  setup do
    register = start_supervised!(StaffCacheRegister)
    # tests share dynamic supervisor, so kill it on test exit
    on_exit(fn -> DynamicSupervisor.stop(CacheSupervisor) end)
    %{register: register}
  end

  # test "spawns agents", %{register: register} do
  #   assert StaffCacheRegister.list(register) == []

  #   StaffCacheRegister.create(register, 5)
  #   caches = StaffCacheRegister.list(register)
  #   assert caches != []
  #   assert length(caches) == 5
  # end

  test "returns only live caches", %{register: register} do
    StaffCacheRegister.create(register, 5)
    cache = StaffCacheRegister.get_cache_by_index(register, 4)
    assert is_pid(cache) == true

    Agent.stop(cache)

    fallback_cache = StaffCacheRegister.get_cache_by_index(register, 4)
    assert fallback_cache != cache
    assert is_pid(fallback_cache) == true
  end

  test "replaces cache on crash", %{register: register} do
    StaffCacheRegister.create(register, 5)
    cache = StaffCacheRegister.get_cache_by_index(register, 1)
    assert is_pid(cache) == true

    # Stop the cache agent with non-normal reason
    Agent.stop(cache, :shutdown)

    assert StaffCacheRegister.get_cache_by_index(register, 1) != cache
  end

  test "refreshes state after agent restart", %{register: register} do
    StaffCacheRegister.create(register, 5)
    cache = StaffCacheRegister.get_cache_by_index(register, 1)
    cache_list_before_shutdown = StaffCacheRegister.list(register)

    Agent.stop(cache, :shutdown)

    cache_list_after_shutdown = StaffCacheRegister.list(register)
    assert length(cache_list_before_shutdown) == length(cache_list_after_shutdown)
  end
end
