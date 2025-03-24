# # Quick recap of implementation. Find full version in README.md file.
# # 1. The main process reads data from a file.
# # 2. It spawns multiple asynchronous processes, each responsible for:
# #    - Receiving a chunk of streamed staff data.
# #    - Building a map from the received lines.
# #    - Transmitting the map back to the main process.
# # 3. The main process merges the received maps and caches the final result in an Agent.
# # 4. It spawns another batch of async processes to read lines from a second file.
# # 5. Each new process performs a lookup against the cached data by querying the Agent.

defmodule LixLookup do
  @pwd "./"
  @all_staff_list @pwd <> "all_staff.csv"
  @region_staff_list @pwd <> "region_staff.csv"
  @region_staff_emails @pwd <> "region_staff_email.csv"

  @max_concurrency System.schedulers()
  @num_caches @max_concurrency
  @proc_time_out 30_000
  @high_read_chunk_size 10_000_000 # 10 MB
  @read_chunk_size 500_000
  @lines_per_chunk 5000

  def run do
    {time, result} = :timer.tc(fn -> main() end)
    IO.puts("Execution time: #{time / 1_000_000} seconds")
    result
  end

  def main() do
    {:ok, cache_register_pid} = StaffCacheRegister.start_link(@num_caches)
    build_staff_map(@all_staff_list, cache_register_pid)

    caches = StaffCacheRegister.list_caches(cache_register_pid)
    match_region_staff_emails(@region_staff_list, caches)
    assemble_matched_staff_and_export_to_csv(@region_staff_emails, caches)
  end

  defp build_staff_map(all_staff, pid) do
    all_staff
    |> FileOps.line_stream_from_chunk_read(@read_chunk_size)
    |> Stream.chunk_every(@lines_per_chunk)
    |> Task.async_stream(&build_and_cache_map(&1, pid), max_concurrency: @max_concurrency, timeout: @proc_time_out)
    |> Stream.run()
  end

  defp build_and_cache_map(chunk_of_lines, reg_pid) when is_list(chunk_of_lines) do
    # Process each line in the chunk
    # merge the results into a single map & cache map in agent
    map =
      Enum.reduce(chunk_of_lines, %{}, fn line, map ->
        case parse_line(line) do
          {:ok, id, email} ->
            Map.put(map, id, String.downcase(email))
          {:error, _} ->
            # IO.puts(reason)
            map
        end
      end)
    StaffCacheRegister.get_cache(reg_pid)
    |> StaffCache.add_staff(map)
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

  def match_region_staff_emails(region_staff, caches) do
    region_staff
    |> FileOps.line_stream_from_chunk_read(@read_chunk_size)
    |> Stream.chunk_every(@lines_per_chunk)
    |> Task.async_stream(&match_staff_to_email(&1, caches),
    max_concurrency: @max_concurrency,
    timeout: @proc_time_out
    )
    |> Stream.run()
  end

  defp match_staff_to_email(staff_list, caches) do
    staff_list
    |> Stream.map(&String.trim(&1))
    |> Stream.map(&String.split(&1, ","))
    |> Enum.reject(fn row -> row == [] end)
    |> (fn staff -> Enum.map(caches, &StaffCache.match_staff(&1, staff)) end).()
  end

  def assemble_matched_staff_and_export_to_csv(path, caches) do
    caches
    |> Stream.map(fn cache -> StaffCache.get_all_matched_staff(cache) end)
    |> FileOps.write_stream_to_csv(path)
  end
end
