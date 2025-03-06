# lix-lookup
A program to read two CSV files:
- the first file contains the staff ID, names, and roles of staff within a specific region.
- the second file contains ID, email, and additional data fields for a larger pool of staff.
the program performs a lookup using staff IDs from the first file
to find their corresponding emails in the second file.
It then generates two new CSV files:
- one containing the staff ID, name, and email for the subset of staff.
  if an email is not found, the email field will contain either "No email" or "Invalid email".
- and another containing the names of those staff whose email were not found
