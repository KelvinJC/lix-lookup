defmodule FileOps do
  @doc """
  Read file at `path` in chunks of bytes - each chunk is the given size \\
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

  def write_stream_to_csv(stream_data, csv_path, opts \\ []) do
    headers = Keyword.get(opts, :headers, [])

    case headers do
      [] -> stream_data
      _ -> Stream.concat(headers, stream_data)
    end
    |> Enum.into(File.stream!(csv_path))
  end
end
