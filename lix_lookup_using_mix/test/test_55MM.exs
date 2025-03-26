defmodule LixLookup55MMRecordsTest do
  use ExUnit.Case
  doctest LixLookup

  @moduletag timeout: :infinity
  @pwd "./"
  @all_staff @pwd <> "lib/big_csv/employee_records_1B_unique_staff_id.csv"
  @region_staff @pwd <> "lib/big_csv/selected_records_32k.csv"
  @region_staff_emails @pwd <> "region_staff_email_55MM.csv"

  test "55 million records" do
    str = LixLookup.main(@all_staff, @region_staff, @region_staff_emails)
    assert is_struct(str)
    assert str.path == "./region_staff_email_55MM.csv"
  end
end
