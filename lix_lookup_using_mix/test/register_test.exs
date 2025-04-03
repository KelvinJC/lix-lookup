defmodule StaffCacheRegisterTest do
  use ExUnit.Case, async: true

  setup do
    register = start_supervised!(StaffCacheRegister)
    %{register: register}
  end

  test "spawns agents", %{register: register} do
    assert StaffCacheRegister.list(register) == []

    StaffCacheRegister.create(register, 5)
    caches = StaffCacheRegister.list(register)
    assert caches != []
    assert length(caches) == 5
  end

  test "returns only live agents", %{register: register} do
    StaffCacheRegister.create(register, 5)
    cache = StaffCacheRegister.get_cache_by_index(register, 4)
    assert is_pid(cache) == true

    Agent.stop(cache)

    fallback_cache = StaffCacheRegister.get_cache_by_index(register, 4)
    assert fallback_cache != cache
    assert is_pid(fallback_cache) == true
  end
end
