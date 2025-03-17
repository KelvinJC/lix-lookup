
defmodule LixLookup do
  @pwd "./"
  @all_staff_list  @pwd<>"all_staff2.csv"
  @region_staff_list  @pwd<>"region_staff_list.csv"
  @region_staff_emails  @pwd<>"region_staff_email.csv"

  def run do
    {time, result} = :timer.tc(fn -> main() end)
    IO.puts("Execution time: #{time / 1_000_000} seconds")
    result
  end

  def main() do
    {:ok, success_process_counter} = ProcessCounter.start_link(0)
    {:ok, error_process_counter} = ProcessCounter.start_link(0)
    {:ok, staff_cache} =
      @all_staff_list
      |> line_stream_from_chunk_read()
      |> Stream.chunk_every(100)
      |> Stream.map(&Task.async(fn ->
        build_map_from_line_stream(&1, success_process_counter, error_process_counter)
      end))
      |> Stream.map(&Task.await(&1))
      |> Enum.reduce(%{}, &Map.merge(&2, &1)) # Merges the results of all tasks
      |> Staff.start_link()

    @region_staff_list
    |> line_stream_from_chunk_read()
    |> Stream.chunk_every(100)
    |> Task.async_stream(
      &match_staff_to_email(staff_cache, &1),
      max_concurrency: 5,
      timeout: :infinity
    )
    |> Enum.reduce([], fn ({:ok, record}, acc) -> acc ++ record end)
    |> Enum.reduce([], fn ({key, value}, acc) -> merge({key, value}, acc) end)
    |> write_stream_to_csv(@region_staff_emails, use_headers: true)
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

  def build_map_from_line_stream(lines, success_counter_pid, error_counter_pid) do
    build =
      try do
        map =
          lines
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
      {:ok, map} ->
        ProcessCounter.increment_count(success_counter_pid)
        map
      {:error, _} ->
        ProcessCounter.increment_count(error_counter_pid)
        %{}
    end
  end

  def format_string(enum) do
    enum
    |> Stream.map(&String.trim(&1))
    |> Stream.map(&String.split(&1, ",", trim: true))
  end

  def match_staff_to_email(cache_pid, staff_list) do
    staff_list
    |> format_string()
    |> Enum.reject(fn (row) -> row == [] end )
    |> Enum.map(&Staff.find_staff_email(&1, cache_pid))
  end

  def merge({key, {id, name, email}}, acc) when key != :error do
    acc ++ ["#{id}, #{String.trim(name)}, #{email}\n"]
  end

  def merge(_, acc) do
    acc
  end

  defp write_stream_to_csv(stream_data, csv_path, opts \\ [use_headers: false]) do
    headers = ["staff_id, name, email\n"]
    use_headers = Keyword.get(opts, :use_headers, false)

    if use_headers == true do
      Stream.concat(headers, stream_data)
    else
      stream_data
    end
    |> Enum.into(File.stream!(csv_path))
  end

  def proc_summary(success_process_counter, error_process_counter) do
    succ = ProcessCounter.get_count(success_process_counter)
    |> IO.inspect(label: "num_successful_processes")
    err = ProcessCounter.get_count(error_process_counter)
    |> IO.inspect(label: "num_error_processes")
    succ + err
    |> IO.inspect(label: "total_num_processes")
  end
end
