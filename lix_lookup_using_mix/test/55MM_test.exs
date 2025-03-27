defmodule LixLookup55MMRecordsTest do
  use ExUnit.Case
  doctest LixLookup

  @moduletag timeout: :infinity

  setup do
    main_args = [
      all_staff: "./lib/big_csv/employee_records_1B_unique_staff_id.csv",
      region_staff: "./lib/big_csv/selected_records_32k.csv",
      region_staff_emails: "./region_staff_email_55MM.csv",
      read_chunk_size: 10_000_000,
      lines_per_chunk: 5000,
      proc_time_out: :infinity
    ]
    %{main_args: main_args}
  end

  test "55 million records", %{main_args: main_args} do
    str = LixLookup.run(main_args)
    assert is_struct(str)
    assert str.path == "./region_staff_email_55MM.csv"
  end
end
