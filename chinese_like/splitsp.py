
import argparse
import os

def split_file(input_file, output_folder):
    if not os.path.exists(input_file):
        print(f"错误: 文件 {input_file} 不存在。")
        exit(1)

    file_size = os.path.getsize(input_file)
    print(f"检测到文件大小: {file_size} 字节")

    if file_size not in [56320, 56384]:
        print("错误: SP文件大小必须为 56320 或 56384 字节。")
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
        print(f"已创建输出目录: {output_folder}")

    with open(input_file, "rb") as f:
        f.seek(skip_bytes)
        for i, size in enumerate(sizes):
            data = f.read(size)
            if len(data) != size:
                print(f"警告: 从偏移 {f.tell()} 读取的数据不足 {size} 字节，实际读取 {len(data)} 字节")
            output_path = os.path.join(output_folder, output_files[i])
            with open(output_path, "wb") as out_f:
                out_f.write(data)
            print(f"已写入 {output_path} ({len(data)} 字节)")

    print("SP文件拆分完成。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="文件拆分工具")
    parser.add_argument("input_file", help="输入文件路径")
    parser.add_argument("output_folder", help="输出文件夹路径")
    args = parser.parse_args()
    split_file(args.input_file, args.output_folder)
