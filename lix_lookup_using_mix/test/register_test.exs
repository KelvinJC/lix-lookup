defmodule StaffCacheRegisterTest do
  use ExUnit.Case, async: true

  setup do
    register = start_supervised!(StaffCacheRegister)
    %{register: register}
  end

  test "spawns agents", %{register: register} do
    assert StaffCacheRegister.list(register) == []

    StaffCacheRegister.create(register, 5)
    pids = StaffCacheRegister.list(register)
    assert pids != []
    assert length(pids) == 5
  end
end
