# A program to read two CSV files:
# - the first file contains the staff ID, names, and roles of staff within a specific region.
# - the second file contains ID, email, and additional data fields for a larger pool of staff.
# the program performs a lookup using staff IDs from the first file
# to find their corresponding emails in the second file.
# It then generates two new CSV files:
# - one containing the staff ID, name, and email for the subset of staff.
#   if an email is not found, the email field will contain either "No email" or "Invalid email".
# - and another containing the names of those staff whose email were not found


defmodule GetStaffEmail do
  @pwd "./"
  @all_staff_list  @pwd<>"all_staff.csv"
  @region_staff_list  @pwd<>"region_staff_list.csv"
  @region_staff_email  @pwd<>"region_staff_email.csv"
  @region_staff_no_email_found  @pwd<>"region_staff_no_email_found.csv"
  @email_length_check "@regionelectricity.com"

  def main() do
    staff_id_email_map =
      read_csv(@all_staff_list)
      |> build_staff_id_email_map()

    region_staff_data = read_csv(@region_staff_list)
    region_staff_data
    |> Stream.map(&match_email(&1, staff_id_email_map))
    |> write_stream_to_csv(@region_staff_email, [use_headers: true])

    staff_id_email_map
    |> get_region_staff_without_email(region_staff_data)
    |> write_stream_to_csv(@region_staff_no_email_found, use_headers: false)
  end

  defp read_csv(file) do
    header_row = 1
    File.stream!(file)
    |> Stream.map(&String.split(&1, ","))
    |> Stream.drop(header_row)
  end

  defp build_staff_id_email_map(all_staff_data) do
    all_staff_data
    |> Enum.reduce(%{}, fn (row, map) ->
      staff_id = Enum.at(row, 5)
      staff_email = Enum.at(row, 7)
      Map.put(map, staff_id, String.downcase(staff_email))
    end)
  end

  defp match_email([_, id, name, _role], staff_id_email_map) do
    email = staff_id_email_map[id]
    email_val = cond do
      email == nil ->
        "No email"
      String.length(email) < String.length(@email_length_check) ->
        "Invalid email"
      true -> email
    end
    "#{id}, #{String.trim(name)}, #{email_val}\n"
  end

  defp write_stream_to_csv(stream_data, csv_path, opts) do
    headers = ["staff_id, name, email\n"]
    use_headers = Keyword.get(opts, :use_headers, false)

    case use_headers do
      true ->
        Stream.concat(headers, stream_data)
      false ->
        stream_data
    end
    |> Enum.into(File.stream!(csv_path))
  end

  defp get_region_staff_without_email(staff_id_email_map, region_staff_data) do
    region_staff_data
    |> Stream.map(fn([_, id, name, _role]) -> {id, name, staff_id_email_map[id]} end)
    |> Stream.filter(fn({_id, _name, email}) ->
      email == nil or
      String.length(email) < String.length(@email_length_check)
    end)
    |> Stream.map(fn({_id, name, _email}) -> "#{String.trim(name)}\n" end)
  end
end


GetStaffEmail.main()
