import argparse
import os
import struct
import traceback

def unpack(input_file, output_folder):
    if os.path.abspath(input_file) == os.path.abspath(output_folder):
        raise ValueError("output cannot be the same as input")
    try:
        if not os.path.isfile(input_file):
            raise ValueError("input file does not exist")
        
        file_size = os.path.getsize(input_file)
        if file_size < 4:
            raise ValueError("input file too small")
        
        with open(input_file, 'rb') as f:
            i_byte = f.read(1)
            if not i_byte:
                raise ValueError("invalid input file: broken header")
            i = ord(i_byte)

            if i == 0 or file_size <= (i * 2 + 1):
                raise ValueError(f"invalid block data (i={i}, block size={file_size})")

            unpack_list = []
            total_blocks_size = 0
            for _ in range(i):
                block_bytes = f.read(2)
                if len(block_bytes) != 2:
                    raise ValueError("invalid input file: cannot get 2 bytes for each block size")
                
                (block_size,) = struct.unpack('<H', block_bytes)
                
                if block_size == 0:
                    raise ValueError("invalid input file: block size is 0")
                
                unpack_list.append(block_size)
                total_blocks_size += block_size

            header_size = 1 + i * 2
            expected_size = header_size + total_blocks_size
            if expected_size > file_size:
                raise ValueError(f"input file size check error, expected size: {expected_size} , actual size: {file_size}")

            os.makedirs(output_folder, exist_ok=True)
            
            for idx, size in enumerate(unpack_list):
                filename = f"{idx:03d}.dat"
                output_path = os.path.join(output_folder, filename)
                
                data = f.read(size)
                if len(data) != size:
                    raise ValueError(f"cannot get enough data for {filename}, expected size: {size}, actual size: {len(data)}")
                
                with open(output_path, 'wb') as out_file:
                    out_file.write(data)

            if remaining := f.read():
                print(f"WARN: input file has extra {len(remaining)} byte(s) leftover")

        print("unpack done")

    except Exception as e:
        print(f"[unpack error] {str(e)}\ntrace: {traceback.format_exc()}")
        exit(1)

def repack(input_folder, output_file):
    if os.path.abspath(input_folder) == os.path.abspath(output_file):
        raise ValueError("output cannot be the same as input")
    try:
        dat_files = []
        
        for fname in os.listdir(input_folder):
            if not fname.lower().endswith('.dat'):
                continue
            try:
                base_name, _ = os.path.splitext(fname)
                index = int(base_name)
            except ValueError:
                continue
            
            file_path = os.path.join(input_folder, fname)
            if os.path.isfile(file_path):
                dat_files.append( (index, fname) )

        if not dat_files:
            raise ValueError("valid .dat file not found")
        
        dat_files.sort(key=lambda x: x)
        i = len(dat_files)
        if i > 255:
            raise ValueError(f"too many .dat files({i}/255)")

        repack_sizes = []
        total_data = 0
        for index, fname in dat_files:
            file_path = os.path.join(input_folder, fname)
            size = os.path.getsize(file_path)
            if size == 0 or size > 0xFFFF:
                raise ValueError(f"invalid file size: {fname} ({size} byte(s))")
            repack_sizes.append(size)
            total_data += size

        with open(output_file, 'wb') as f:
            f.write(bytes([i]))
            
            for size in repack_sizes:
                f.write(struct.pack('<H', size))
            
            for _, fname in dat_files:
                with open(os.path.join(input_folder, fname), 'rb') as infile:
                    f.write(infile.read())

        expected_size = 1 + 2*i + total_data
        if (actual := os.path.getsize(output_file)) != expected_size:
            raise ValueError(f"repack check failed, expected size: {expected_size} , actual size: {actual}")

        print("repack done")

    except Exception as e:
        print(f"[repack error] {str(e)}\ntrace: {traceback.format_exc()}")
        exit(1)

def parse(input_file, output_file):
    if os.path.abspath(input_file) == os.path.abspath(output_file):
        raise ValueError("output cannot be the same as input")
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            pass

        with open(input_file, 'rb') as f_in:
            p_byte = f_in.read(1)
            if not p_byte:
                raise ValueError("invalid input_file: empty file")
            
            p = ord(p_byte)
            if p == 0:
                raise ValueError("invalid input_file: first byte is 0x00")

            data = f_in.read(2 * p)
            if len(data) != 2 * p:
                raise ValueError("invalid input_file: incomplete header")

            A = []
            B = []
            C = 0
            for i in range(p):
                a = ord(data[2*i:2*i+1])
                b = ord(data[2*i+1:2*i+2])
                
                if a > 2:
                    raise ValueError(f"invalid input_file: type value of A{i+1} {a}>2")
                if b == 0:
                    raise ValueError(f"invalid input_file: length value of B{i+1} is 0")
                
                A.append(a)
                B.append(b)
                C += b

            f_in.seek(0, os.SEEK_END)
            T = f_in.tell()
            remaining = T - (2 * p + 1)
            
            if remaining <= 0 or remaining % C != 0:
                raise ValueError("invalid input_file: file size error")

            E = [f"{A[i]},{B[i]}" for i in range(p)]
            
            with open(output_file, 'w', encoding='utf-8') as f_out:
                f_out.write("\t".join(E) + "\n")

            f_in.seek(2 * p + 1)
            iterations = remaining // C
            line_number = 1
            
            with open(output_file, 'a', encoding='utf-8') as f_out:
                for _ in range(iterations):
                    line_number += 1
                    S = []
                    for i in range(p):
                        b = B[i]
                        chunk = f_in.read(b)
                        if len(chunk) != b:
                            with open(output_file, 'w', encoding='utf-8') as f:
                                pass
                            raise ValueError("invalid input_file: incomplete data")
                        
                        if A[i] == 2:
                            filtered_bytes = bytes([byte for byte in chunk if byte != 0x00])
                            if len(filtered_bytes) % 2 != 0:
                                print(f"[WARNING] column {i+1} @ line {line_number}: "
                                      f"non-zero data byte length is odd ({len(filtered_bytes)}), this may cause issue sometimes, use single-byte space for padding if necessary")
                            try:
                                decoded_str = filtered_bytes.decode('shift-jis')
                                S.append(decoded_str)
                            except UnicodeDecodeError:
                                hex_str = "".join([f"{byte:02X}" for byte in filtered_bytes])
                                S.append(hex_str)
                        else:
                            hex_str = "".join([f"{byte:02X}" for byte in chunk])
                            S.append(hex_str)
                    
                    f_out.write("\t".join(S) + "\n")
        print("MHi common data file parsed")
    except Exception as e:
        print(f"[parse error] {str(e)}\ntrace: {traceback.format_exc()}")
        with open(output_file, 'w', encoding='utf-8') as f:
            pass
        return

def validate_input_line(line):
    parts = line.strip().split('\t')
    if not parts:
        return None, "invalid input_file: cannot find any TAB(\t)"
    n = len(parts)
    if n > 255:
        return None, "invalid input_file: too many columns (n>255)"
    a_list = []
    b_list = []
    for part in parts:
        try:
            a, b = part.split(',')
            a = int(a)
            b = int(b)
            if a > 2:
                return None, f"invalid input_file: A{a}>2"
            if b == 0 or b > 255:
                return None, f"invalid input_file: B{b} not in range [1-255]"
            a_list.append(a)
            b_list.append(b)
        except ValueError:
            return None, f"invalid input_file: '{part}' is not a valid [A,B] format"
    return (n, a_list, b_list), None

def process_text_column(text, b, line_number, col_index):
    try:
        encoded_bytes = text.encode('shift-jis')
        if len(encoded_bytes) > b:
            return None, f"invalid input_file: line {line_number} @ column {col_index+1} string too long (max: {b} bytes)"
        non_zero_bytes = [byte for byte in encoded_bytes if byte != 0x00]
        if len(non_zero_bytes) % 2 != 0:
            print(f"[WARNING] line {line_number} @ column {col_index+1}: "
                  f"non-zero byte length is odd ({len(non_zero_bytes)}), this may cause issue sometimes, use single-byte space for padding if necessary")
        if len(encoded_bytes) < b:
            encoded_bytes += bytes([0x00] * (b - len(encoded_bytes)))
        return encoded_bytes, None
    except UnicodeEncodeError:
        return None, f"invalid input_file: line {line_number} @ column {col_index+1} contains invalid Shift-JIS characters"

def process_file(input_file, output_file):
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'wb') as outfile:
            first_line = infile.readline()
            if not first_line:
                return "invalid input_file: empty file"
            result, error = validate_input_line(first_line)
            if error:
                return error
            n, a_list, b_list = result
            outfile.write(bytes([n]))
            for a, b in zip(a_list, b_list):
                outfile.write(bytes([a]))
                outfile.write(bytes([b]))
            line_number = 1
            for line in infile:
                line_number += 1
                line = line.strip('\n')
                if not line:
                    break
                parts = line.split('\t')
                if len(parts) != n:
                    return f"invalid input_file: line {line_number} expected {n} columns, got {len(parts)}"
                for i, (text, a, b) in enumerate(zip(parts, a_list, b_list)):
                    if a == 2:
                        encoded_bytes, error = process_text_column(text, b, line_number, i)
                        if error:
                            return error
                        outfile.write(encoded_bytes)
                    else:
                        if len(text) != 2 * b:
                            return f"invalid input_file: line {line_number} @ column {i+1} expected byte length: {2*b}, actual byte length: {len(text)}"
                        try:
                            hex_bytes = bytes.fromhex(text)
                        except ValueError:
                            return f"invalid input_file: line {line_number} @ column {i+1} contains non HEX[0-9A-F] character"
                        outfile.write(hex_bytes)
            return None
    except IOError as e:
        return f"file operating error: {str(e)}"

def build(input_file, output_file):
    if os.path.abspath(input_file) == os.path.abspath(output_file):
        raise ValueError("output cannot be the same as input")
    error = process_file(input_file, output_file)
    
    if error:
        if os.path.exists(output_file):
            os.remove(output_file)
        print(f"[build error]: {error}")
        print("invalid input_file")
        exit(1)
    else:
        print(f"MHi common data file build done")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="MHi common package/data file unpack/repack/parse/build tool (v2.2)")
    subparsers = parser.add_subparsers(dest='command', required=True)

    unpack_parser = subparsers.add_parser('unpack', 
        help='unpack MHi package file',
        description='try to unpack file to destination folder using MHi package file format')
    unpack_parser.add_argument('input_file', help='input MHi package file')
    unpack_parser.add_argument('output_folder', help='output folder for unpacked files')

    repack_parser = subparsers.add_parser('repack',
        help='repack files to MHi package file',
        description='use <number>.dat for filenames inside the input folder, repack will use the same order')
    repack_parser.add_argument('input_folder', help='input folder which contains .dat files waiting to be repacked')
    repack_parser.add_argument('output_file', help='output MHi package file')

    parse_parser = subparsers.add_parser('parse',
        help='parse MHi common data file and output',
        description='try to parse using MHi common data file(not package file) structure and output all the data to text file')
    parse_parser.add_argument('input_file', help='MHi file waiting to be parsed')
    parse_parser.add_argument('output_file', help='output text file')

    build_parser = subparsers.add_parser('build',
        help='build MHi common data file and output',
        description='try to build output file from valid text file using MHi common data file(not package file) structure')
    build_parser.add_argument('input_file', help='text file which output file will be build from')
    build_parser.add_argument('output_file', help='output MHi common data file')

    args = parser.parse_args()

    try:
        if args.command == 'unpack':
            unpack(args.input_file, args.output_folder)
        elif args.command == 'repack':
            repack(args.input_folder, args.output_file)
        elif args.command == 'parse':
            parse(args.input_file, args.output_file)
        elif args.command == 'build':
            build(args.input_file, args.output_file)
    except Exception as e:
        print(f"EXIT: {str(e)}\n{traceback.format_exc()}")
        exit(1)
