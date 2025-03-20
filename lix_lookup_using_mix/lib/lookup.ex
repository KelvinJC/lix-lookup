# Quick recap of implementation. Find full version in README.md file.
# 1. The main process reads data from a file.
# 2. It spawns multiple asynchronous processes, each responsible for:
#    - Receiving a chunk of streamed staff data.
#    - Building a map from the received lines.
#    - Transmitting the map back to the main process.
# 3. The main process merges the received maps and caches the final result in an Agent.
# 4. It spawns another batch of async processes to read lines from a second file.
# 5. Each new process performs a lookup against the cached data by querying the Agent.

defmodule LixLookup do
  @pwd "./"
  @all_staff_list  @pwd<>"all_staff.csv"
  @region_staff_list  @pwd<>"region_staff.csv"
  @region_staff_emails  @pwd<>"region_staff_email.csv"

  def run do
    {time, result} = :timer.tc(fn -> main() end)
    IO.puts("Execution time: #{time / 1_000_000} seconds")
    result
  end

  def main() do
    {:ok, staff_cache_pid} = build_staff_map(@all_staff_list)
    write_region_staff_data(@region_staff_list, staff_cache_pid, @region_staff_emails)
  end

  defp build_staff_map(all_staff) do
    all_staff
    |> line_stream_from_chunk_read()
    |> Stream.chunk_every(2500)
    |> Task.async_stream(&build_map_from_line_stream/1, max_concurrency: 5, timeout: :infinity)
    |> Enum.reduce(%{}, fn ({:ok, stream_result}, acc) -> Map.merge(acc, stream_result) end)
    |> Staff.start_link()
  end

  defp write_region_staff_data(region_staff, staff_cache_pid, path) do
    region_staff
    |> line_stream_from_chunk_read()
    |> Stream.chunk_every(500)
    |> Task.async_stream(&match_staff_to_email(&1, staff_cache_pid), max_concurrency: 300, timeout: :infinity)
    |> Enum.reduce([], fn ({:ok, stream_result}, acc) -> acc ++ merge_stream_result_enum(stream_result) end)
    |> write_stream_to_csv(path, use_headers: true)
  end

  def merge_stream_result_enum(stream_result_enum) do
    Enum.reduce(stream_result_enum, [], fn {tag, row}, lines ->
      case tag do
        :ok -> lines ++ [row]
        :error -> lines
      end
    end)
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
      new_chunk = acc <> chunk |> String.split("\n", trim: true)

      case new_chunk do
        [] -> {[], ""}
        [last_line] -> {[], last_line}
        [last_line | lines] -> {lines, last_line}
      end
    end)
  end

  defp build_map_from_line_stream(line_stream) do
    build =
      try do
        map =
          line_stream
          |> format_strings()
          |> Enum.reduce(%{}, fn (row, map) ->
            [_, _, _, _, _, id, _, email | _] = row
            Map.put(map, id, String.downcase(email))
          end)
        {:ok, map}
      rescue
        e -> {:error, Exception.message(e)}
      end

    case build do
      {:ok, map} -> map
      {:error, _} -> %{}
    end
  end

  defp format_strings(strings) do
    strings
    |> Stream.map(&String.trim(&1))
    |> Stream.map(&String.split(&1, ","))
  end

  defp match_staff_to_email(staff_list, cache_pid) do
    staff_list
    |> format_strings()
    |> Enum.reject(fn (row) -> row == [] end )
    |> Staff.lookup_staff_emails(cache_pid)
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

defmodule Staff do
  def start_link(map) do
    Agent.start_link(fn -> map end)
  end

  def get_all_staff(agent) do
    Agent.get(agent, fn(state) -> state end)
  end

  def find_staff_email([_, id, name, _], agent) do
    Agent.get(agent, fn(state) ->
      email = Map.get(state, id)
      if email == nil do
        {:error, {:email_not_found}}
      else
        {:ok, {id, name, email}}
      end
    end)
  end

  def lookup_staff_emails(staff, agent) do
    lookup = fn staff, all_staff ->
      Enum.map(staff, fn ([_, id, name, _]) ->
        email = Map.get(all_staff, id)
        if email == nil do
          {:error, "Staff ID does not match any record."}
        else
          {:ok, "#{id}, #{String.trim(name)}, #{email}\n"}
        end
      end)
    end

    Agent.get(agent, fn(all_staff) -> lookup.(staff, all_staff) end)
  end
end
