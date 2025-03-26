defmodule LixLookupTest do
  use ExUnit.Case
  doctest LixLookup

  @pwd "./"
  @all_staff @pwd <> "all_staff_1MM.csv"
  @region_staff @pwd <> "region_staff.csv"
  @region_staff_emails @pwd <> "region_staff_email.csv"

  test "1 million records" do
    str = LixLookup.main(@all_staff, @region_staff, @region_staff_emails)
    assert is_struct(str)
    assert str.path == "./region_staff_email.csv"
  end
end
