import os
import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox



def read_file(file_path):
    with open(file_path, 'r') as file:
        return file.readlines()


def compare_files(file1_lines, file2_lines, max_unmatched, columns_to_omit, has_header):
    import csv

    unmatched_count = 0
    match_count = 0
    unmatched_records = []

    file1_total_records = len(file1_lines)
    file2_total_records = len(file2_lines)

    # Determine the delimiter (simple sniff from file1 first line)
    sample_line = file1_lines[0]
    delimiter = ',' if ',' in sample_line else ';'

    # If header exists, skip first line for comparison
    start_index = 1 if has_header else 0

    for i in range(start_index, min(len(file1_lines), len(file2_lines))):
        line1 = file1_lines[i].strip()
        line2 = file2_lines[i].strip()

        row1 = next(csv.reader([line1], delimiter=delimiter))
        row2 = next(csv.reader([line2], delimiter=delimiter))

        # Normalize field counts to the shorter of the two
        min_len = min(len(row1), len(row2))
        row1 = row1[:min_len]
        row2 = row2[:min_len]

        # Remove columns marked for omission
        compare_fields1 = [val for idx, val in enumerate(row1) if idx not in columns_to_omit]
        compare_fields2 = [val for idx, val in enumerate(row2) if idx not in columns_to_omit]

        # Join the remaining fields back into a comparable string
        reconstructed1 = delimiter.join(compare_fields1)
        reconstructed2 = delimiter.join(compare_fields2)

        if reconstructed1 != reconstructed2:
            unmatched_count += 1
            if unmatched_count > max_unmatched:
                break
            unmatched_records.append((i + 1, i + 1, line1, line2))
        else:
            match_count += 1

    return match_count, unmatched_count, unmatched_records, file1_total_records, file2_total_records


def format_unmatched_records(unmatched_records):
    formatted_output = ""
    for record in unmatched_records:
        file1_rec_num, file2_rec_num, file1_line, file2_line = record

        # Record numbers and headings
        formatted_output += f"\nFILE1 RECORD NUMBER: {file1_rec_num}\n"
        formatted_output += f"FILE2 RECORD NUMBER: {file2_rec_num}\n"

        # Compare and print data in chunks of 80 characters
        max_length = max(len(file1_line), len(file2_line))  # Compare until the longest line is fully checked
        for start in range(0, max_length, 80):
            # Slice the current 80-character chunk, making sure to pad to 80 characters for both parts
            part1 = file1_line[start:start + 80]
            part2 = file2_line[start:start + 80]

            # Print the column range and hash marks, shifting 9 characters for alignment
            col_start = start + 1
            col_end = min(start + 80, max_length)
            end_marker = "(end)" if col_end == max_length else "(continued)"
            formatted_output += f"         Col\t{col_start:03d} - {col_end:03d}{end_marker}\n"
            formatted_output += "         ----+----1----+----2----+----3----+----4----+----5----+----6----+----7----+----8" + f"{end_marker}\n"

            # Print data from FILE1 and FILE2
            formatted_output += f"(FILE1): {part1.rstrip()}\n"
            formatted_output += f"(FILE2): {part2.rstrip()}\n"

            # Mismatch indicators - shift by 9 characters and ensure first mismatches are shown
            mismatch_indicators = "".join("~" if c1 != c2 else " " for c1, c2 in zip(part1.ljust(80), part2.ljust(80)))
            formatted_output += f"         {mismatch_indicators.rstrip()}\n"

        formatted_output += "\n"
    return formatted_output

def launch_compare_gui():
    root = tk.Tk()
    root.title("Two File Compare Utility")
    # root.geometry("600x150")
    selected_dir = None
    file1_path = ""
    file2_path = ""
    checkbox_vars = []  # stores checkbox states
    header_frame = None
    has_header = False  # properly tracks user header input
    output_text = None  # reference to scrollable output box

    max_unmatched_var = tk.StringVar( value="50" )

    tk.Label(root, text="Step 1: Select directory with two files to compare").pack(pady=10)

    def select_directory():
        nonlocal file1_path, file2_path

        selected_dir = filedialog.askdirectory(title="Select directory containing the two files")
        if not selected_dir:
            return
        files = [f for f in os.listdir( selected_dir ) if os.path.isfile( os.path.join( selected_dir, f ) )]
        if len( files ) != 2:
            messagebox.showerror( "Error", f"Directory must contain exactly TWO files.\nFound: {len( files )}" )
            return

        # Store full file paths
        file1_path = os.path.join( selected_dir, files[0] )
        file2_path = os.path.join( selected_dir, files[1] )

        # Ask: Do these files have headers?
        header_response = tk.simpledialog.askstring( "Header Row", "Do these files contain headers? [Y/N]:" )
        if not header_response or header_response.upper() not in ["Y", "N"]:
            messagebox.showerror( "Error", "Invalid response. Please enter Y or N." )
            return

        nonlocal has_header
        has_header = header_response.upper() == "Y"

        # Read the first file's first line
        with open( file1_path, 'r' ) as f:
            first_line = f.readline().strip()

        delimiter = ',' if ',' in first_line else ';'

        if has_header:
            import csv
            header = next( csv.reader( [first_line], delimiter=delimiter ) )
            field_list = [f"{i + 1}. {col}" for i, col in enumerate( header )]
        else:
            field_list = [f"Column {i + 1}" for i in range( len( first_line.split( delimiter ) ) )]

        # Show field list in a simple message box for now
        messagebox.showinfo( "Detected Columns",
                             "\n".join( field_list[:20] ) + ("\n..." if len( field_list ) > 20 else "") )
        show_column_checkboxes( field_list )

    def show_column_checkboxes(field_list):
        nonlocal header_frame, checkbox_vars
        if header_frame:
            header_frame.destroy()  # Remove any previous field list
        header_frame = tk.Frame( root )
        header_frame.pack( pady=10 )

        checkbox_vars.clear()
        for field in field_list:
            var = tk.IntVar( value=1 )
            cb = tk.Checkbutton( header_frame, text=field, variable=var, anchor="w", width=50 )
            cb.pack( anchor="w" )
            checkbox_vars.append( var )

        # Add Select All / Deselect All buttons
        def select_all():
            for var in checkbox_vars:
                var.set( 1 )

        def deselect_all():
            for var in checkbox_vars:
                var.set( 0 )

        tk.Button( header_frame, text="Select All", command=select_all ).pack( side="left", padx=5 )
        tk.Button( header_frame, text="Deselect All", command=deselect_all ).pack( side="left", padx=5 )

    def run_compare():
        nonlocal file1_path, file2_path, checkbox_vars

        if not file1_path or not file2_path:
            messagebox.showerror( "Error", "No files selected." )
            return

        try:
            max_unmatched = int( max_unmatched_var.get() )
        except ValueError:
            messagebox.showerror( "Error", "Max unmatched must be a valid integer." )
            return

        # Determine columns to OMIT based on unchecked boxes
        columns_to_omit = {i for i, var in enumerate( checkbox_vars ) if var.get() == 0}

        # Read file lines
        file1_lines = read_file( file1_path )
        file2_lines = read_file( file2_path )

        match_count, unmatched_count, unmatched_records, total1, total2 = compare_files(
            file1_lines, file2_lines, max_unmatched, columns_to_omit, has_header
        )

        out_path = os.path.join( os.path.dirname( file1_path ), "COMPARE_RESULTS.txt" )
        with open( out_path, "w", newline="" ) as out_file:
            out_file.write( f"FILE1: {os.path.basename( file1_path )}\n" )
            out_file.write( f"FILE2: {os.path.basename( file2_path )}\n" )
            if unmatched_count == 0:
                out_file.write( "ALL RECORDS IN BOTH FILES MATCHED\n" )
            else:
                out_file.write( f"PROCESSING ENDED AFTER {unmatched_count} UNMATCHED FOUND\n" )
            out_file.write( f"TOTAL NUMBER OF RECORDS IN FILE1 READ: {total1}\n" )
            out_file.write( f"TOTAL NUMBER OF RECORDS IN FILE2 READ: {total2}\n" )
            if unmatched_records:
                out_file.write( format_unmatched_records( unmatched_records ) )

        # Show summary popup
        # Write summary to GUI output box
        output_text.delete( 1.0, tk.END )
        output_text.insert( tk.END, f"FILE1: {os.path.basename( file1_path )}\n" )
        output_text.insert( tk.END, f"FILE2: {os.path.basename( file2_path )}\n" )
        output_text.insert( tk.END, f"{match_count} record(s) matched\n" )
        output_text.insert( tk.END, f"{unmatched_count} record(s) differed\n" )
        output_text.insert( tk.END, f"Output written to: {out_path}\n" )

    # Frame for unmatched entry and compare button
    compare_frame = tk.Frame( root )
    compare_frame.pack( pady=10 )

    tk.Label( compare_frame, text="Max unmatched records before stopping:" ).pack( side="left", padx=5 )
    tk.Entry( compare_frame, textvariable=max_unmatched_var, width=5 ).pack( side="left", padx=5 )

    tk.Button( compare_frame, text="Run Compare", command=lambda: run_compare() ).pack( side="left", padx=10 )

    tk.Button(root, text="Select Directory", command=select_directory).pack()
    # Output text area (scrollable)
    output_frame = tk.Frame( root )
    output_frame.pack( fill="both", expand=True, padx=10, pady=10 )

    output_scrollbar = tk.Scrollbar( output_frame )
    output_scrollbar.pack( side="right", fill="y" )

    output_text = tk.Text( output_frame, height=10, wrap="word", yscrollcommand=output_scrollbar.set )
    output_text.pack( side="left", fill="both", expand=True )

    output_scrollbar.config( command=output_text.yview )

    root.mainloop()


if __name__ == "__main__":
    launch_compare_gui()
