import argparse
import os
import struct
import traceback

def unpack(input_file, output_folder):
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
                raise ValueError(f"invalid block data (i = {i} , block size = {file_size})")

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
                raise ValueError(f"input file size check error, expect size: {expected_size} , actual size: {file_size}")

            os.makedirs(output_folder, exist_ok=True)
            
            for idx, size in enumerate(unpack_list):
                filename = f"{idx:03d}.dat"
                output_path = os.path.join(output_folder, filename)
                
                data = f.read(size)
                if len(data) != size:
                    raise ValueError(f"cannot get enough data for {filename} , expect size: {size} , actual size: {len(data)}")
                
                with open(output_path, 'wb') as out_file:
                    out_file.write(data)

            if remaining := f.read():
                print(f"WARN: input file has extra {len(remaining)} byte(s) leftover")

        print("unpack done")

    except Exception as e:
        print(f"[unpack error] {str(e)}\ntrace: {traceback.format_exc()}")
        exit(1)

def repack(input_folder, output_file):
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
            raise ValueError(f"repack check failed, expect size: {expected_size} , actual size: {actual}")

        print("repack done")

    except Exception as e:
        print(f"[repack error] {str(e)}\ntrace: {traceback.format_exc()}")
        exit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="MHi tool for unpacking/repacking common data files (v1.1)")
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    unpack_parser = subparsers.add_parser('unpack')
    unpack_parser.add_argument('input_file')
    unpack_parser.add_argument('output_folder')
    
    repack_parser = subparsers.add_parser('repack')
    repack_parser.add_argument('input_folder')
    repack_parser.add_argument('output_file')
    
    args = parser.parse_args()
    
    try:
        if args.command == 'unpack':
            unpack(args.input_file, args.output_folder)
        elif args.command == 'repack':
            repack(args.input_folder, args.output_file)
    except Exception as e:
        print(f"EXIT: {str(e)}\n{traceback.format_exc()}")
        exit(1)
