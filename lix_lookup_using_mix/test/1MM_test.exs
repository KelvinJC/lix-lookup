defmodule LixLookupTest do
  use ExUnit.Case
  doctest LixLookup

  setup do
    main_args = [
      all_staff: "./all_staff_1MM.csv",
      region_staff: "./region_staff.csv",
      region_staff_emails: "./region_staff_email_1MM.csv",
      read_chunk_size: 500_000,
      lines_per_chunk: 5000,
      proc_time_out: 30_000
    ]

    %{main_args: main_args}
  end

  test "1 million records", %{main_args: main_args} do
    str = LixLookup.run(main_args)
    assert is_struct(str)
    assert str.path == "./region_staff_email_1MM.csv"
  end
end
