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

  def run do
    {time, result} = :timer.tc(fn -> main() end)
    IO.puts("Execution time: #{time / 1_000_000} seconds")
    result
  end

  def main() do
    {:ok, cache_register_pid} = StaffCacheRegister.start_link(8)
    build_staff_map(@all_staff_list, cache_register_pid)
    match_region_staff_emails_and_write_to_csv(@region_staff_list, @region_staff_emails, cache_register_pid)
  end

  defp build_staff_map(all_staff, pid) do
    all_staff
    |> line_stream_from_chunk_read()
    |> Stream.chunk_every(5000)
    |> Task.async_stream(&build_and_cache_map(&1, pid), max_concurrency: 8, timeout: :infinity)
    |> Stream.run()
  end

  @doc """
    Processes data of staff in region, matches each staff member to an email using a cache,
    and writes the matched data to a CSV file.

    ## Parameters
    - `region_staff`: The input data CSV file path containing region staff.
    - `path`: The file path where the CSV output will be written.
    - `staff_cache_pid`: The PID of the cache process used to look up staff emails.
  """
  def match_region_staff_emails_and_write_to_csv(region_staff, path, reg_pid) do
    region_staff
    |> line_stream_from_chunk_read()
    |> Stream.chunk_every(5000)
    |> Task.async_stream(&match_staff_to_email(&1, reg_pid),
      max_concurrency: 8,
      timeout: 30_000
    )
    |> Stream.run()

    # TODO: need to assemble data from all Agents
    # StaffCache.get_all_matched_staff(staff_cache_pid)
    # |> write_stream_to_csv(path, use_headers: true)
  end

  @doc """
  Read file at `path` in chunks of given size (binary mode) \\
  and output a new stream of lines from each chunk. \\
  Default value of `chunk_size` is 500 KB.
  """
  def line_stream_from_chunk_read(path, chunk_size \\ 10_000_000) do
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

  defp parse_line(line) do
    # Split the line into parts and extract the ID and email
    case String.split(line, ",") do
      [_, _, _, _, _, id, _, email | _] ->
        {:ok, id, email}
      _ ->
        {:error, "Invalid line format: #{inspect(line)}"}
    end
  end

  defp match_staff_to_email(staff_list, reg_pid) do
    cache_pid = StaffCacheRegister.get_next_cache(reg_pid)

    staff_list
    |> Stream.map(&String.trim(&1))
    |> Stream.map(&String.split(&1, ","))
    |> Enum.reject(fn row -> row == [] end)
    |> StaffCache.match_staff_id_to_emails(cache_pid)
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

defmodule StaffCacheRegister do
  def start_link(num_caches) do
    caches = create_caches(num_caches)
    [next_cache | rest_caches] = caches
    Agent.start(fn -> {next_cache, rest_caches, caches} end)
  end

  defp create_caches(num) do
    Enum.map(1..num, fn _ -> StaffCache.start_link() end)
    |> Enum.map(fn {:ok, cache_pid} -> cache_pid end)
  end

  def get_all_caches do
    Agent.get(agent, fn {_, _, caches} -> caches end)
  end

  def get_next_cache(agent) do
    {next_cache, _, _} = get_and_update(agent)
    next_cache
  end

  defp get_and_update(agent) do
    Agent.get_and_update(agent,
      fn {_current_cache, rest_caches, caches} = old_state ->
      new_state = loop_through_caches(rest_caches, caches)
      {old_state, new_state}
    end)
  end

  defp loop_through_caches(previous_remaining_caches, original_caches) do
    # If previous_remaining_caches is empty, reset to the original list of caches
    new_remaining_caches =
      if previous_remaining_caches == [] do
        original_caches
      else
        previous_remaining_caches
      end

    [next_cache | rest_caches] = new_remaining_caches
    {next_cache, rest_caches, original_caches}
  end
end

defmodule StaffCache do
  def start_link() do
    Agent.start_link(fn -> {%{}, []} end)
  end

  def get_state(agent) do
    Agent.get(agent, fn {all_staff, matched_staff} -> {all_staff, matched_staff} end)
  end

  def get_all_staff(agent) do
    Agent.get(agent, fn {all_staff, _matched_staff} -> all_staff end)
  end

  def get_all_matched_staff(agent) do
    Agent.get(agent, fn {_all_staff, matched_staff} -> matched_staff end)
  end

  def update_all_staff_map(agent, staff) when is_map(staff) do
    Agent.update(agent, fn {all_staff, matched_staff} ->
      {Map.merge(staff, all_staff), matched_staff}
    end)
  end

  def match_staff_id_to_emails(staff, agent) do
    lookup = fn staff, all_staff ->
      staff
      |> Enum.map(fn [_, id, name, _] ->
          email = Map.get(all_staff, id)
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

    Agent.update(agent, fn {all_staff, matched_staff} ->
      new_matched_staff = lookup.(staff, all_staff)
      {all_staff, matched_staff ++ new_matched_staff}
    end)
  end
end
