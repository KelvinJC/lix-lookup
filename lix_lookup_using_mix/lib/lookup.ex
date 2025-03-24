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
  # @all_staff_list @pwd <> "lib/employee_records_1B_unique_staff_id.csv"
  # @region_staff_list @pwd <> "lib/selected_records_32k.csv"
  # @all_staff_list @pwd <> "all_staff_1MM.csv"
  @all_staff_list @pwd <> "all_staff.csv"
  @region_staff_list @pwd <> "region_staff.csv"
  @region_staff_emails @pwd <> "region_staff_email.csv"

  def run do
    {time, result} = :timer.tc(fn -> main() end)
    IO.puts("Execution time: #{time / 1_000_000} seconds")
    result
  end

  def main() do
    {:ok, cache_register_pid} = StaffCacheRegister.start_link(8)
    build_staff_map(@all_staff_list, cache_register_pid)
    match_region_staff_emails(@region_staff_list, cache_register_pid)
    assemble_matched_staff_and_export_to_csv(@region_staff_emails, cache_register_pid)
  end

  defp build_staff_map(all_staff, pid) do
    all_staff
    |> line_stream_from_chunk_read()
    |> Stream.chunk_every(5000)
    |> Task.async_stream(&build_and_cache_map(&1, pid), max_concurrency: 8, timeout: 30_000)
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
    StaffCacheRegister.get_next_cache(reg_pid)
    |> StaffCache.update_all_staff_map(map)
  end

  def match_region_staff_emails(region_staff, reg_pid) do
    caches = StaffCacheRegister.get_all_caches(reg_pid)

    region_staff
    |> line_stream_from_chunk_read()
    |> Stream.chunk_every(5000)
    |> Task.async_stream(&match_staff_to_email(&1, caches),
    max_concurrency: 8,
    timeout: 30_000
    )
    |> Stream.run()
  end

  defp match_staff_to_email(staff_list, caches) do
    staff_list
    |> Stream.map(&String.trim(&1))
    |> Stream.map(&String.split(&1, ","))
    |> Enum.reject(fn row -> row == [] end)
    |> (fn staff -> Enum.map(caches, &StaffCache.match_staff_id_to_emails(staff, &1)) end).()
  end

  def assemble_matched_staff_and_export_to_csv(path, reg_pid) do
    StaffCacheRegister.get_all_caches(reg_pid)
    |> Stream.map(fn cache -> StaffCache.get_all_matched_staff(cache) end)
    |> write_stream_to_csv(path, use_headers: true)
  end

  @doc """
  Read file at `path` in chunks of given size (binary mode) \\
  and output a new stream of lines from each chunk. \\
  Default value of `chunk_size` is 500 KB.
  """
  def line_stream_from_chunk_read(path, chunk_size \\ 500_000) do
    File.stream!(path, [], chunk_size)
    |> Stream.transform("", fn chunk, acc ->
      chunk = String.replace(chunk, "\r\n", "\n")
      new_chunk = (acc <> chunk) |> String.split("\n", trim: true)

      case new_chunk do
        [] -> {[], ""}
        [last_line] -> {[], last_line}
        [last_line | lines] -> {lines, last_line}
      end
    end)
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

  defp write_stream_to_csv(stream_data, csv_path, opts) do
    headers = ["staff_id, name, email\n"]
    use_headers = Keyword.get(opts, :use_headers, false)

    if use_headers == true do
      Stream.concat(headers, stream_data)
    else
      stream_data
    end
    |> Enum.into(File.stream!(csv_path))
  end
end
