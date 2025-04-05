# Quick recap of implementation.
# 1. At the start of the application, a GenServer process `StaffCacheRegister` is kickstarted by a Supervisor process.
#    It is responsible for
#    - triggering a Dynamic Supervisor to generate and monitor multiple Agent processes
#    - storing the PIDs of these agent processes which serve as in-memory caches.
# 2. At the start of the program, the initial process streams rows of data from a file.
# 3. It spawns multiple asynchronous processes, each responsible for:
#    - Receiving and parsing rows of staff data.
#    - Constructing a key-value map from the parsed rows.
#    - Querying the `StaffCacheRegister` for the PID of a `StaffCache` process
#    - Sending the map to the `StaffCache` process for caching.
# 4. Then streams lines of data from a second file.
# 5. It spawns another batch of async processes, each responsible for:
#    - Receiving rows of streamed staff data.
#    - Querying each `StaffCache` agent process to match staff with their emails.
# 6. Each `StaffCache` process matches staff to their email records by performing lookups of each line against
#    the key value map in its internal state.
#    - It maintains a list of matched staff records.
# 7. The initial process retrieves the matched data from all caches and exports it to a CSV file.

defmodule LixLookup do
  @pwd "./"
  @default_all_staff @pwd <> "all_staff.csv"
  @default_region_staff @pwd <> "region_staff.csv"
  @default_region_staff_emails @pwd <> "region_staff_email.csv"
  @default_lines_per_chunk 5000
  @default_read_chunk_size 500_000 # 500 KB
  @default_proc_time_out 30_000    # 30,000 milliseconds == 30 seconds
  @max_concurrency System.schedulers_online()
  @num_caches 10

  def run(args \\ []) do
    {time, result} =
      :timer.tc(fn ->
        parse_args(args)
        |> main()
      end)

    IO.puts("Execution time: #{time / 1_000_000} seconds")
    result
  end

  defp parse_args(args) when is_list(args) do
    all_staff = Keyword.get(args, :all_staff, @default_all_staff)
    region_staff = Keyword.get(args, :region_staff, @default_region_staff)
    region_staff_emails = Keyword.get(args, :region_staff_emails, @default_region_staff_emails)
    read_chunk_size = Keyword.get(args, :read_chunk_size, @default_read_chunk_size)
    lines_per_chunk = Keyword.get(args, :lines_per_chunk, @default_lines_per_chunk)
    proc_time_out = Keyword.get(args, :proc_time_out, @default_proc_time_out)

    {all_staff, region_staff, region_staff_emails, read_chunk_size, lines_per_chunk,
     proc_time_out}
  end

  def main(args) do
    {all_staff, region_staff, region_staff_emails, read_chunk_size, lines_per_chunk,
     proc_time_out} = args

    StaffCacheRegister.create(CacheRegister, @num_caches)

    stream_read(all_staff, read_chunk_size, lines_per_chunk)
    |> cache_staff_data(CacheRegister, proc_time_out)

    stream_read(region_staff, read_chunk_size, lines_per_chunk)
    |> match_region_staff_emails(CacheRegister, proc_time_out)

    assemble_matched_staff_and_export_to_csv(CacheRegister, region_staff_emails)
  end

  defp stream_read(path, chunk_size, lines_per_chunk) do
    path
    |> FileOps.line_stream_from_chunk_read(chunk_size)
    |> Stream.chunk_every(lines_per_chunk)
  end

  defp cache_staff_data(all_staff, pid, time_out) do
    all_staff
    |> Task.async_stream(&cache_valid_line(&1, pid),
      max_concurrency: @max_concurrency,
      timeout: time_out,
      on_timeout: :kill_task
    )
    |> Stream.run()
  end

  defp cache_valid_line(chunk_of_lines, reg_pid) when is_list(chunk_of_lines) do
    for line <- chunk_of_lines do
      case parse_line(line) do
        {:ok, id, email} ->
          i = Cache.put(id, email)
          IO.inspect(i, label: "result from Cache.put")

        {:error, _} ->
        IO.inspect("dud here!")
      end
    end
  end

  defp parse_line(line) do
    # Split the line into parts and extract the ID and email
    case String.split(line, ",") do
      [_, _, _, _, _, id, _, email | _] ->
        {:ok, id, email}

      _ ->
        {:error, "Invalid line format: #{inspect(line)}"}
    end
  end

  def match_region_staff_emails(region_staff, pid, time_out) do
    region_staff
    |> Task.async_stream(&match(&1, pid),
      max_concurrency: @max_concurrency,
      timeout: time_out,
      on_timeout: :kill_task
    )
    |> Stream.run()
  end

  defp match(staff_list, reg_pid) do
    staff_list
    |> Enum.map(fn [_, id, name, _] ->
      email = get(id)

      if email == nil do
        {:error, "Staff ID does not match any record."}
      else
        {:ok, "#{id}, #{String.trim(name)}, #{email}\n"}
      end
    end)
    |> Enum.map(fn {tag, row} ->
      case tag do
        :ok -> row
        :error -> []
      end
    end)
  end

  def assemble_matched_staff_and_export_to_csv(reg_pid, path) do
    res =
      StaffCacheRegister.list(reg_pid)
      |> Stream.map(fn cache -> StaffCache.get_all_matched_staff(cache) end)
      |> FileOps.write_stream_to_csv(path, headers: ["staff_id, name, email\n"])

    StaffCacheRegister.clear_all(reg_pid) # clear all caches against next program run
    res
  end
end
