# Quick recap of implementation.
# 1. At the start of the application, a GenServer process `Cache` is kickstarted by a Supervisor process.
#    - It is responsible for creating an ETS table serve as in-memory cache.
# 2. When the run/1 function is called, the initial process streams rows of data from a file.
# 3. It spawns multiple asynchronous processes, each responsible for:
#    - Receiving and parsing rows of staff data.
#    - Caching the id and email field from each parsed rows.
# 4. Then streams lines of data from a second file.
# 5. It spawns another batch of async processes, each responsible for:
#    - Receiving rows of streamed staff data.
#    - Performs lookups on the ETS `:all_staff` table to match staff with their emails.
# 6. The initial process retrieves the matched data from all async processes and exports it to a CSV file.

defmodule LixLookup do
  @pwd "./"
  @default_all_staff @pwd <> "all_staff.csv"
  @default_region_staff @pwd <> "region_staff.csv"
  @default_region_staff_emails @pwd <> "region_staff_email.csv"
  @default_lines_per_chunk 5000
  @default_read_chunk_size 500_000 # 500 KB
  @default_proc_time_out 30_000    # 30,000 milliseconds == 30 seconds
  @max_concurrency System.schedulers_online()

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

    stream_read(all_staff, read_chunk_size, lines_per_chunk)
    |> cache_staff_data(proc_time_out)

    stream_read(region_staff, read_chunk_size, lines_per_chunk)
    |> fetch_region_staff_emails(proc_time_out)
    |> export_to_csv(region_staff_emails)
  end

  defp stream_read(path, chunk_size, lines_per_chunk) do
    path
    |> FileOps.line_stream_from_chunk_read(chunk_size)
    |> Stream.chunk_every(lines_per_chunk)
  end

  defp cache_staff_data(all_staff, time_out) do
    all_staff
    |> Task.async_stream(&cache_valid_line(&1),
      max_concurrency: @max_concurrency,
      timeout: time_out,
      on_timeout: :kill_task
    )
    |> Stream.run()
  end

  defp cache_valid_line(chunk_of_lines) when is_list(chunk_of_lines) do
    for line <- chunk_of_lines do
      case parse_line(line) do
        {:ok, id, email} ->
          Cache.put(id, email)
        {:error, _} ->
          nil
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

  def fetch_region_staff_emails(region_staff, time_out) do
    region_staff
    |> Task.async_stream(&match_staff_id_to_email(&1),
      max_concurrency: @max_concurrency,
      timeout: time_out,
      on_timeout: :kill_task
    )
    |> Enum.flat_map(fn
      {:ok, records} -> records
      _ -> []
    end)
  end

  defp match_staff_id_to_email(staff_list) do
    staff_list
    |> Stream.map(&String.trim(&1))
    |> Stream.map(&String.split(&1, ","))
    |> Stream.map(fn [_, staff_id, name, _] ->
      email = Cache.get(staff_id)

      if email == nil do
        nil
      else
        "#{staff_id}, #{String.trim(name)}, #{email}\n"
      end
    end)
    |> Stream.reject(fn row -> row == :nil end)
  end

  def export_to_csv(matched_staff, path) do
    FileOps.write_stream_to_csv(matched_staff, path, headers: ["staff_id, name, email\n"])
  end
end
