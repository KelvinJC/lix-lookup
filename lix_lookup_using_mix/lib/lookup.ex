# Quick recap of implementation.
# 1. The main process creates an agent process `StaffCacheRegister` responsible for
#    generating and tracking multiple agent processes to serve as memory caches.
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
  @all_staff_list @pwd <> "all_staff.csv"
  @region_staff_list @pwd <> "region_staff.csv"
  @region_staff_emails @pwd <> "region_staff_email.csv"
  @max_concurrency System.schedulers_online()
  @num_caches 10
  @lines_per_chunk 5000
  @read_chunk_size 500_000  # 500 KB
  @proc_time_out 30_000     # 30,000 milliseconds == 30 seconds


  # -- for use in tests with CSV files in excess of 55 million records
  # @high_read_chunk_size 10_000_000 # 10 MB
  # @read_chunk_size @high_read_chunk_size
  # @high_proc_time_out :infinity
  # @proc_time_out @high_proc_time_out

  def run do
    {time, result} = :timer.tc(fn -> main() end)
    IO.puts("Execution time: #{time / 1_000_000} seconds")
    result
  end

  def main() do
    {:ok, cache_register_pid} = StaffCacheRegister.start_link(@num_caches)
    build_staff_map(@all_staff_list, cache_register_pid)

    caches = StaffCacheRegister.list_caches(cache_register_pid)
    match_region_staff_emails(@region_staff_list, cache_register_pid)
    assemble_matched_staff_and_export_to_csv(@region_staff_emails, caches)
  end

  defp build_staff_map(all_staff, pid) do
    all_staff
    |> FileOps.line_stream_from_chunk_read(@read_chunk_size)
    |> Stream.chunk_every(@lines_per_chunk)
    |> Task.async_stream(&build_and_cache_sorted_map(&1, pid),
      max_concurrency: @max_concurrency,
      timeout: @proc_time_out,
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

  def match_region_staff_emails(region_staff, pid) do
    region_staff
    |> FileOps.line_stream_from_chunk_read(@read_chunk_size)
    |> Stream.chunk_every(@lines_per_chunk)
    |> Task.async_stream(&match_per_cache(&1, pid),
      max_concurrency: @max_concurrency,
      timeout: @proc_time_out,
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

  def assemble_matched_staff_and_export_to_csv(path, caches) do
    caches
    |> Stream.map(fn cache -> StaffCache.get_all_matched_staff(cache) end)
    |> FileOps.write_stream_to_csv(path, headers: ["staff_id, name, email\n"])
  end
end
