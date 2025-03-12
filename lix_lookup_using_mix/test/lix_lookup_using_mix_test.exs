defmodule LixLookupUsingMixTest do
  use ExUnit.Case
  doctest LixLookupUsingMix

  test "all runs smoothly" do
    str = LixLookupUsingMix.main()
    assert is_struct(str)
    assert str.path == "./region_staff_no_email_found.csv"
  end
end
