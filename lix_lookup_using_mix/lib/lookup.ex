# Implementation:
# 1. The main process reads data from a file.
# 2. It spawns multiple asynchronous processes, each responsible for:
#    - Receiving a chunk of streamed staff data.
#    - Building a map from the received lines.
#    - Transmitting the map back to the main process.
# 3. The main process merges the received maps and caches the final result in an Agent.
# 4. Another batch of processes reads lines from a second file.
# 5. Each process performs a lookup against the cached data by querying the Agent.

defmodule LixLookup do
  @pwd "./"
  @all_staff_list  @pwd<>"all_staff.csv"
  @region_staff_list  @pwd<>"region_staff_list.csv"
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

  def build_staff_map(all_staff) do
    all_staff
    |> line_stream_from_chunk_read()
    |> Stream.chunk_every(2500)
    |> Stream.map(&Task.async(fn -> build_map_from_line_stream(&1) end))
    |> Stream.map(&Task.await(&1))
    |> Enum.reduce(%{}, &Map.merge(&2, &1)) # merge results from all tasks
    |> Staff.start_link()
  end

  def write_region_staff_data(region_staff, staff_cache_pid, path) do
    region_staff
    |> line_stream_from_chunk_read()
    |> Stream.chunk_every(100)
    |> Task.async_stream(&match_staff_to_email(&1, staff_cache_pid), max_concurrency: 5, timeout: :infinity)
    |> Enum.reduce([], fn ({:ok, stream_result}, acc) -> acc ++ stream_result end)
    |> Enum.reduce([], fn ({tag, row}, acc) -> merge({tag, row}, acc) end)
    |> write_stream_to_csv(path, use_headers: true)
  end

  @doc """
  Read file at `path` in chunks of given size (binary mode) \\
  and output a new stream of lines from each chunk. \\
  Default value of `chunk_size` is 500 KB.
  """
  def line_stream_from_chunk_read(path, chunk_size \\ 500_000) do
    path
    |> File.stream!([], chunk_size)
    |> Stream.transform("", fn (chunk, acc) ->
      [last_line | lines] =
        acc <> chunk
        |> String.split("\n")
      {lines, last_line}
    end)
  end

  def build_map_from_line_stream(line_stream) do
    build =
      try do
        map =
          line_stream
          |> format_string()
          |> Stream.map(fn([_, _, _, _, _, id, _, email | _]) ->
            %{id => String.downcase(email)}
          end)
          |> Enum.reduce(%{}, fn (new_map, old_map) ->
            Map.merge(new_map, old_map)
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

  def format_string(enum) do
    enum
    |> Stream.map(&String.trim(&1))
    |> Stream.map(&String.split(&1, ",", trim: true))
  end

  def match_staff_to_email(staff_list, cache_pid) do
    staff_list
    |> format_string()
    |> Enum.reject(fn (row) -> row == [] end )
    |> Enum.map(&Staff.find_staff_email(&1, cache_pid))
  end

  def merge({tag, {id, name, email}}, acc) when tag != :error do
    acc ++ ["#{id}, #{String.trim(name)}, #{email}\n"]
  end

  def merge(_, acc) do
    acc
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

  def find_staff_email([_, id, name, _], agent) do
    Agent.get(agent, fn (state) ->
      email = Map.get(state, id)
      if email == nil do
        {:error, {:email_not_found}}
      else
        {:ok, {id, name, email}}
      end
    end)
  end

  def get_map(agent) do
    Agent.get(agent, fn (state) -> state end)
  end
end
