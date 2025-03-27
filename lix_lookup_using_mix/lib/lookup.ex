# Quick recap of implementation.
# 1. The main process creates an agent process `StaffCacheRegister` responsible for
#    generating and tracking multiple agent processes to serve as in-memory caches.
# 2. The main process streams rows of data from a file.
# 3. It spawns multiple asynchronous processes, each responsible for:
#    - Receiving and parsing rows of staff data.
#    - Constructing a key-value map from the parsed rows.
#    - Querying the `StaffCacheRegister` for the PID of a `StaffCache` process
#    - Sending the map to the `StaffCache` process for caching.
# 4. The main process streams lines of data from a second file.
# 5. It spawns another batch of async processes, each responsible for:
#    - Receiving rows of streamed staff data.
#    - Querying each `StaffCache` agent process to match staff with their emails.
# 6. Each `StaffCache` process matches staff to their email records by performing lookups of each line against
#    the key value map in its internal state.
#    - It maintains a list of matched staff records.
# 7. The main process retrieves the matched data from all caches and exports it to a CSV file.

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

    {:ok, cache_register_pid} = StaffCacheRegister.start_link(@num_caches)

    stream_read(all_staff, read_chunk_size, lines_per_chunk)
    |> build_staff_map(cache_register_pid, proc_time_out)

    stream_read(region_staff, read_chunk_size, lines_per_chunk)
    |> match_region_staff_emails(cache_register_pid, proc_time_out)

    assemble_matched_staff_and_export_to_csv(cache_register_pid, region_staff_emails)
  end

  defp stream_read(path, chunk_size, lines_per_chunk) do
    path
    |> FileOps.line_stream_from_chunk_read(chunk_size)
    |> Stream.chunk_every(lines_per_chunk)
  end

  defp build_staff_map(all_staff, pid, time_out) do
    all_staff
    |> Task.async_stream(&build_and_cache_sorted_map(&1, pid),
      max_concurrency: @max_concurrency,
      timeout: time_out,
      on_timeout: :kill_task
    )
    |> Stream.run()
  end

  defp build_and_cache_sorted_map(chunk_of_lines, reg_pid) when is_list(chunk_of_lines) do
    # Process each line in the chunk
    # sort the id field
    # merge the results into a single map & cache map in agent

    map =
      for line <- chunk_of_lines,
          reduce: %{} do
        sorted_map ->
          case parse_line(line) do
            {:ok, id, email} ->
              add_to_sorted_map(id, String.downcase(email), sorted_map)

            {:error, _} ->
              sorted_map
          end
      end

    for {index, records} <- map,
        {int_index, _} = Integer.parse(index) do
      StaffCacheRegister.get_cache_by_index(reg_pid, int_index)
      |> StaffCache.add_staff(records)
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

  defp add_to_sorted_map(id, email, sorted_map) do
    index = String.first(id)
    staff_records = Map.get(sorted_map, index, %{})
    new_staff_records = Map.put_new(staff_records, id, email)
    Map.put(sorted_map, index, new_staff_records)
  end

  def match_region_staff_emails(region_staff, pid, time_out) do
    region_staff
    |> Task.async_stream(&match_per_cache(&1, pid),
      max_concurrency: @max_concurrency,
      timeout: time_out,
      on_timeout: :kill_task
    )
    |> Stream.run()
  end

  defp match_per_cache(staff_list, reg_pid) do
    staff_list
    |> Stream.map(&String.trim(&1))
    |> Stream.map(&String.split(&1, ","))
    |> Enum.group_by(fn [_, id, _, _] -> String.first(id) end)
    |> Enum.each(fn {index, records} ->
      {int_index, _} = Integer.parse(index)

      StaffCacheRegister.get_cache_by_index(reg_pid, int_index)
      |> StaffCache.match_staff(records)
    end)
  end

  def assemble_matched_staff_and_export_to_csv(reg_pid, path) do
    StaffCacheRegister.list_caches(reg_pid)
    |> Stream.map(fn cache -> StaffCache.get_all_matched_staff(cache) end)
    |> FileOps.write_stream_to_csv(path, headers: ["staff_id, name, email\n"])
  end
end
