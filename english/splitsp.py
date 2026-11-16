
import argparse
import os

def split_file(input_file, output_folder):
    if not os.path.exists(input_file):
        print(f"ERR: file {input_file} does not exist.")
        exit(1)

    file_size = os.path.getsize(input_file)
    print(f"Detected file size: {file_size} bytes")

    if file_size not in [56320, 56384]:
        print("ERR: SP file size must be 56320 or 56384 bytes")
        exit(1)

    output_files = [
        "00_pcx_gard_yy.dat",
        "01_gard_yy.dat",
        "02_arm_xxx.dat",
        "03_m_pcx.dat",
        "04_savedata.dat"
    ]

    skip_bytes = 32 if file_size == 56320 else 96
    sizes = [14336, 7600, 1500, 28400, 1500]

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Output folder created: {output_folder}")

    with open(input_file, "rb") as f:
        f.seek(skip_bytes)
        for i, size in enumerate(sizes):
            data = f.read(size)
            if len(data) != size:
                print(f"WARN: data size from offset {f.tell()} is less than {size} bytes, actual size is {len(data)} bytes")
            output_path = os.path.join(output_folder, output_files[i])
            with open(output_path, "wb") as out_f:
                out_f.write(data)
            print(f"Written to {output_path} ({len(data)} bytes)")

    print("SP file split done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SP file splitter for MHi_for_SH")
    parser.add_argument("input_file", help="input file path")
    parser.add_argument("output_folder", help="output folder path")
    args = parser.parse_args()
    split_file(args.input_file, args.output_folder)
