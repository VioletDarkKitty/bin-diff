#!/usr/bin/python3

import sys
import subprocess
from colorama import Style, Fore
import gc
import os
import tempfile

def read_valid(read_val, valids):
	return read_val is not None and read_val in valids

def print_hex_diff(buf_1, buf_2, beginning_index):
	length = len(buf_1) - 1
	FIXED_WIDTH_PRINT_LEN = 16
	printed_len_1 = 0
	printed_len_2 = 0
	printed_len_1_a = 0
	printed_len_2_a = 0
	printed_index = 0
	
	while True:
		if printed_index == 0 or printed_index % 128 == 0:
			print("\n%s" % format(beginning_index + printed_index, "8x"))
		
		print(f"{Fore.GREEN}F1:\t{Style.RESET_ALL}", end="")
		printed_total = 0
		for i in range(FIXED_WIDTH_PRINT_LEN):
			if buf_1[printed_index + i] == buf_2[printed_index + i]:
				print(f"%s " % (buf_1[printed_index + i].hex()), end="")
			else:
				print(f"{Fore.GREEN}%s {Style.RESET_ALL}" % (buf_1[printed_index + i].hex()), end="")
			printed_len_1 += 1
			printed_total += 1
			
			if printed_len_1 > length:
				break
		print("   " * (FIXED_WIDTH_PRINT_LEN - printed_total), end="")
		
		print("\t", end="")
		for i in range(FIXED_WIDTH_PRINT_LEN):
			print_val = None
			try:
				print_val = buf_1[printed_index + i].decode("utf-8")
			except:
				pass
			if print_val is None or not print_val.isprintable():
				print_val = "."
			if buf_1[printed_index + i] == buf_2[printed_index + i]:
				print(f"%s" % (print_val), end="")
			else:
				print(f"{Fore.GREEN}%s{Style.RESET_ALL}" % (print_val), end="")
			printed_len_1_a += 1
			
			if printed_len_1_a > length:
				break
		
		print(f"\n{Fore.RED}F2:\t{Style.RESET_ALL}", end="")
		printed_total = 0
		for i in range(FIXED_WIDTH_PRINT_LEN):
			if buf_1[printed_index + i] == buf_2[printed_index + i]:
				print(f"%s " % (buf_2[printed_index + i].hex()), end="")
			else:
				print(f"{Fore.RED}%s {Style.RESET_ALL}" % (buf_2[printed_index + i].hex()), end="")
			printed_len_2 += 1
			printed_total += 1
			
			if printed_len_2 > length:
				break
		print("   " * (FIXED_WIDTH_PRINT_LEN - printed_total), end="")
		
		print("\t", end="")
		for i in range(FIXED_WIDTH_PRINT_LEN):
			print_val = None
			try:
				print_val = buf_2[printed_index + i].decode("utf-8")
			except:
				pass
			if print_val is None or not print_val.isprintable():
				print_val = "."
			if buf_1[printed_index + i] == buf_2[printed_index + i]:
				print(f"%s" % (print_val), end="")
			else:
				print(f"{Fore.RED}%s{Style.RESET_ALL}" % (print_val), end="")
			printed_len_2_a += 1
			
			if printed_len_2_a > length:
				break
		
		printed_index += FIXED_WIDTH_PRINT_LEN
		
		if printed_len_1 > length:
			break
		
		print("")
	
	print("\n")

def buffer_read_file(f, length):
	return f.read(length)

def commit_buffer(f, write_buffer):
	f.write(b''.join(write_buffer))
	gc.collect()

def buffered_write_file(f, length, write_buffer, byte_array, delay_write):
	write_length = len(write_buffer)
	written = 0
	for byte in byte_array:
		write_buffer.append(byte)
		written += 1
		
		if write_length + written == length:
			if not delay_write:
				commit_buffer(f, write_buffer)
				write_buffer = []
	
	return write_buffer

def sublist(val, _list):
	# discussion of speed for the below function
	# https://stackoverflow.com/questions/10106901/elegant-find-sub-list-in-list
	for i in range(len(_list)):
		if _list[i] == val[0] and _list[i:i+len(val)] == val:
			return True
	return False

def diff_merge(f1_path, f2_path, output_path, interactive, long_diff, delayed_write, dvdisaster_mode, null_replace):
	f1 = open(f1_path, "rb")
	f2 = open(f2_path, "rb")
	of = open(output_path, "wb")
	
	f1_eof = False
	f2_eof = False
	fill_to_end = False
	differ = False
	diff_buffer_f1 = []
	diff_buffer_f2 = []
	total_diff_bytes = 0
	newly_eof = False
	byte_index = 0
	
	LONG_DIFF_LEN = 16
	current_match_len = 0
	
	dvdisaster_vals = [
		"Dead sector number",
		"Dead sector marker",
		"This sector",
		"could not be read",
		"substituted with",
		"the dvdisaster",
		"dvdisaster read",
		"read routine",
		"Medium fingerprint",
		"Volume label",
		"dvdisaster dead",
		"sector marker"
	]
	dvdisaster_val_bytes = []
	for val in dvdisaster_vals:
		bytes_val = []
		for char in val:
			bytes_val.append(bytes(char, 'utf-8'))
		dvdisaster_val_bytes.append(bytes_val)
	
	write_buffer = []
	continue_buffering = True
	BUFFER_LENGTH = 1024 * 1024 * 250 # 250MiB
	while continue_buffering:
		buffer_1 = buffer_read_file(f1, BUFFER_LENGTH)
		buffer_2 = buffer_read_file(f2, BUFFER_LENGTH)
		
		
		#while True:
		for buf_index in range(BUFFER_LENGTH):
			#b1 = f1.read(1)
			#b2 = f2.read(1)
			b1 = buffer_1[buf_index:buf_index+1]
			b2 = buffer_2[buf_index:buf_index+1]
			
			eof_file = None
			other_file = None
			if not b1:
				eof_file = f1_path
				other_file = f2_path
				f1_eof = True
				newly_eof = True
			if not b2:
				eof_file = f2_path
				other_file = f1_path
				f2_eof = True
				newly_eof = True
			
			if f1_eof and f2_eof:
				continue_buffering = False
				break
			
			if f1_eof or f2_eof:
				if newly_eof and not fill_to_end:
					print("EOF for %s" % eof_file)
					if interactive:
						print("Continue with bytes from %s [Y/n]? > " % (other_file), end="")
						
						read_val = None
						while not read_valid(read_val, "ynYN"):
							if read_val is not None:
								print("Invalid\n>", end="")
							read_val = input()
						
						if read_val in "Yy":
							fill_to_end = True
						else:
							continue_buffering = False
							break
					else:
						print("Continuing to end with %s bytes" % (other_file))
						fill_to_end = True
					
					newly_eof = False
				
				if fill_to_end:
					if f1_eof:
						write_buffer = buffered_write_file(of, BUFFER_LENGTH, write_buffer, [b2], delayed_write)
						#of.write(b2)
					elif f2_eof:
						#of.write(b1)
						write_buffer = buffered_write_file(of, BUFFER_LENGTH, write_buffer, [b1], delayed_write)
			
			leave_equality_check = False
			do_buffer_write = False
			if b1 == b2:
				if differ:
					if long_diff:
						if current_match_len < LONG_DIFF_LEN:
							# we want to skip matching some bytes to make much longer matches if there are lots of small differences
							# just add to the buffer instead
							leave_equality_check = True
							current_match_len += 1
						else:
							current_match_len = 0
					
					if not leave_equality_check:
						# print differ to ask for confirm if we're in interactive mode
						diff_length = len(diff_buffer_f1)
						begin_hex = format(byte_index - 1, "x")
						end_hex = format(byte_index - 1 + diff_length, "x")
						
						auto_kept = False
						if null_replace:
							f1_null = all([x == b'\0' for x in diff_buffer_f1])
							f2_null = all([x == b'\0' for x in diff_buffer_f2])
							
							keep = None
							if f1_null and not f2_null:
								keep = True
							elif f2_null and not f1_null:
								keep = False
							
							if keep is not None:
								auto_kept = True
								kept_file_path = ""
								if keep:
									kept_file_path = f2_path
									write_buffer = buffered_write_file(of, BUFFER_LENGTH, write_buffer, diff_buffer_f2, delayed_write)
								else:
									kept_file_path = f1_path
									write_buffer = buffered_write_file(of, BUFFER_LENGTH, write_buffer, diff_buffer_f1, delayed_write)
								print("%d bytes differ\t(%s - %s). Auto keeping non-null data from %s..." % (diff_length, begin_hex, end_hex, kept_file_path))
								
								diff_buffer_f1 = []
								diff_buffer_f2 = []
								differ = False
						
						if not auto_kept:
							if dvdisaster_mode:
								for val in dvdisaster_val_bytes:
									in_f1 = sublist(val, diff_buffer_f1)
									in_f2 = sublist(val, diff_buffer_f2)
									
									keep = None
									if in_f1 and not in_f2:
										keep = True
									elif in_f2 and not in_f1:
										keep = False
									
									if keep is not None:
										auto_kept = True
										kept_file_path = ""
										if keep:
											kept_file_path = f2_path
											write_buffer = buffered_write_file(of, BUFFER_LENGTH, write_buffer, diff_buffer_f2, delayed_write)
										else:
											kept_file_path = f1_path
											write_buffer = buffered_write_file(of, BUFFER_LENGTH, write_buffer, diff_buffer_f1, delayed_write)
										print("%d bytes differ\t(%s - %s). Auto keeping non-dvdisaster data from %s..." % (diff_length, begin_hex, end_hex, kept_file_path))
										
										diff_buffer_f1 = []
										diff_buffer_f2 = []
										differ = False
						
						if not auto_kept:
							if interactive:
								repeat = True
								while repeat:
									print("%d bytes differ\t(%s - %s).\nKeep which (1/2, p to print, h for more help)? > " % (diff_length, begin_hex, end_hex), end="")
									
									read_val = None
									while not read_valid(read_val, "12pPhHeEqQ"):
										if read_val is not None:
											print("Invalid\n> ", end="")
										read_val = input()
									
									if read_val == "1":
										repeat = False
										write_buffer = buffered_write_file(of, BUFFER_LENGTH, write_buffer, diff_buffer_f1, delayed_write)
										#for c in diff_buffer_f1:
										#	of.write(c)
									elif read_val == "2":
										repeat = False
										#for c in diff_buffer_f2:
										#	of.write(c)
										write_buffer = buffered_write_file(of, BUFFER_LENGTH, write_buffer, diff_buffer_f2, delayed_write)
									elif read_val in "eE":
										editor = os.environ.get("EDITOR")
										if editor is None:
											editor = "/usr/bin/editor"
										
										while True:
											print("Editing changes with [default: %s]: " % (editor), end="")
											new_editor = input()
											if new_editor == "":
												new_editor = editor
											print("All text left in the edited file will be written to the output, regardless of length")
											
											temp_file = tempfile.NamedTemporaryFile()
											commit_buffer(temp_file, diff_buffer_f1 + diff_buffer_f2)
											
											subprocess.run([new_editor, temp_file.name])
											
											read_buffer = []
											temp_file.seek(0)
											total_read_length = 0
											while True:
												read = buffer_read_file(temp_file, BUFFER_LENGTH)
												read_buffer.append(read)
												read_length = len(read)
												total_read_length += read_length
												if read_length != BUFFER_LENGTH:
													break
											
											print("Read %d bytes. Keep [y/n]? " % (total_read_length), end="")
											continue_val = None
											while not read_valid(continue_val, "ynYN"):
												if continue_val is not None:
													print("Invalid\n> ", end="")
												continue_val = input()
											
											if continue_val in "Yy":
												repeat = False
												temp_file.close()
												write_buffer = buffered_write_file(of, BUFFER_LENGTH, write_buffer, read_buffer, delayed_write)
											
											break
									elif read_val in "qQ":
										exit()
									elif read_val in "pP":
										print_hex_diff(diff_buffer_f1, diff_buffer_f2, byte_index - 1)
									else:
										print(("1 - Keep changes from %s\n" % (f1_path))
											+ ("2 - Keep changes from %s\n" % (f2_path))
											+ "p - Print a hex difference of the changes\n"
											+ "h - Print this help\n"
											+ "e - Edit this change manually\n"
											+ "q - Quit"
										)
								
								diff_buffer_f1 = []
								diff_buffer_f2 = []
								differ = False
							else:
								print("%d bytes differ\t(%s - %s). Keeping %s bytes..." % (diff_length, begin_hex, end_hex, f2_path))
								
								write_buffer = buffered_write_file(of, BUFFER_LENGTH, write_buffer, diff_buffer_f2, delayed_write)
								#for c in diff_buffer_f2:
								#	of.write(c)
								
								diff_buffer_f1 = []
								diff_buffer_f2 = []
								differ = False
				
				if not leave_equality_check:
					write_buffer = buffered_write_file(of, BUFFER_LENGTH, write_buffer, [b1], delayed_write)
					#of.write(b1)
			else:
				current_match_len = 0
				do_buffer_write = True
			
			if do_buffer_write or leave_equality_check:
				total_diff_bytes += 1
				diff_buffer_f1.append(b1)
				diff_buffer_f2.append(b2)
				differ = True
			
			byte_index += 1
	
	if len(write_buffer) != 0:
		commit_buffer(of, write_buffer)
	
	f1.close()
	f2.close()
	of.close()
	
	return total_diff_bytes

if __name__ == "__main__":
	if len(sys.argv) < 4:
		print("Usage: %s [file 1] [file 2] [output] [(optional) option string]\n" % (sys.argv[0])
			+ "Where option string:\n"
			+ "\tn - Non-Interactive Merge (all changes from file 2 are kept)\n"
			+ "\tl - Long Difference Mode (ignore up to 16 bytes of matched when looking for differences)\n"
			+ "\td - Delayed Write (Delay writing to the output file until everything has been read)\n"
			+ "\tz - Dvdisaster dead sector mode (Keep any change that does not include dead sectors from dvdisaster)\n"
			+ "\tc - Non-null replacement mode (Keep any change that is not entirely null bytes)"
		) 
		exit()
	
	option_interactive = True
	option_long_diff = False
	option_delay_write = False
	option_dvdisaster_mode = False
	option_null_replace = False
	if len(sys.argv) >= 5:
		for option in sys.argv[4]:
			if option in "lL":
				option_long_diff = True
				print("Long Difference Mode")
			elif option in "nN":
				option_interactive = False
				print("Non-Interactive Merge")
			elif option in "dD":
				option_delay_write = True
				print("Delayed Write")
			elif option in "zZ":
				option_dvdisaster_mode = True
				print("Dvdisaster dead sector mode")
			elif option in "c":
				option_null_replace = True
				print("Non-null replacement mode")
			else:
				print("Unknown option %s" % (option))
				exit()
	
	try:
		print("Diff merge %s and %s to %s" % (sys.argv[1], sys.argv[2], sys.argv[3]))
		total = diff_merge(sys.argv[1], sys.argv[2], sys.argv[3], option_interactive, option_long_diff, option_delay_write, option_dvdisaster_mode, option_null_replace)
		print("Finished. Total changed bytes: %d" % total)
	except KeyboardInterrupt:
		print(f"{Style.RESET_ALL}")
