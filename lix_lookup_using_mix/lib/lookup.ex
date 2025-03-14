
defmodule LixLookup do
  @pwd "./"
  @all_staff_list  @pwd<>"all_staff.csv"

  def main() do
    @all_staff_list
    |> line_stream_from_chunk_read()
    |> Stream.chunk_every(100)
    |> Stream.map(&Task.async(fn -> build_map_from_line_stream(&1) end))
    |> Stream.map(&Task.await(&1))
    |> Enum.reduce(%{}, &Map.merge(&2, &1)) # Merges the results of all tasks
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

  def build_map_from_line_stream(lines) do
    build =
      try do
        map =
          lines
          |> Stream.map(&String.trim/1)
          |> Stream.map(&String.split(&1, ","))
          # |> Stream.reject(fn row -> Enum.at(row, 7) == nil end)
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
end
