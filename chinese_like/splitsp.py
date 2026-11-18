import argparse
import os
import re

DEFAULT_OUTPUT_FILES = [
    "pcx_gard_yy.dat",
    "gard_yy.dat",
    "arm_xxx.dat",
    "m_pcx.dat",
    "savedata.dat",
]

SIZES = [14336, 7600, 1500, 28400, 1500]

NAME_START = 0x21
DOT_BYTE = 0x2E
NUL_BYTE = 0x00
MAX_NAME_LEN = 64


def clean_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = name.strip().rstrip(".")
    return name


def read_name_from_header(data: bytes, index: int) -> str:
    if len(data) <= NAME_START:
        raise ValueError(
            f"错误: 第 {index} 个块长度不足，无法从 0x{NAME_START:02X} 读取文件头名称"
        )

    buf = bytearray()
    pos = NAME_START
    end = min(len(data), NAME_START + MAX_NAME_LEN)

    while pos < end:
        b = data[pos]
        if b == DOT_BYTE or b == NUL_BYTE:
            break
        buf.append(b)
        pos += 1

    return buf.decode("ascii", errors="ignore")


def get_output_filename(data: bytes, index: int, fallback: str) -> str:
    if index == 4:
        return "savedata.dat"

    raw_name = read_name_from_header(data, index)
    name = clean_filename(raw_name)

    if not name:
        print(f"警告: 第 {index} 个文件头中未解析到有效文件名，使用默认名 {fallback}")
        name = os.path.splitext(fallback)[0]

    return f"{name}.dat"


def split_file(input_file: str, output_folder: str) -> None:
    if not os.path.exists(input_file):
        print(f"错误: 文件 {input_file} 不存在。")
        exit(1)

    file_size = os.path.getsize(input_file)
    print(f"检测到文件大小: {file_size} 字节")

    if file_size not in [56320, 56384]:
        print("错误: SP文件大小必须为 56320 或 56384 字节。")
        exit(1)

    skip_bytes = 32 if file_size == 56320 else 96

    os.makedirs(output_folder, exist_ok=True)
    print(f"输出目录: {output_folder}")

    used_names = set()

    with open(input_file, "rb") as f:
        f.seek(skip_bytes)

        for i, size in enumerate(SIZES):
            data = f.read(size)

            if len(data) != size:
                print(
                    f"警告: 从偏移 {f.tell()} 读取的数据不足 {size} 字节，"
                    f"实际读取 {len(data)} 字节"
                )

            fallback = DEFAULT_OUTPUT_FILES[i]

            try:
                output_name = get_output_filename(data, i, fallback)
            except ValueError as e:
                print(f"错误: {e}")
                exit(1)

            if output_name in used_names:
                stem, ext = os.path.splitext(output_name)
                n = 2
                while f"{stem}_{n}{ext}" in used_names:
                    n += 1
                new_name = f"{stem}_{n}{ext}"
                print(f"警告: 输出文件名 {output_name} 重复，改为 {new_name}")
                output_name = new_name

            used_names.add(output_name)

            output_path = os.path.join(output_folder, output_name)

            with open(output_path, "wb") as out_f:
                out_f.write(data)

            print(f"已写入 {output_path} ({len(data)} 字节)")

    print("SP文件拆分完成。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MHi_for_SH SP文件拆分工具")
    parser.add_argument("input_file", help="输入文件路径")
    parser.add_argument("output_folder", help="输出目录路径")
    args = parser.parse_args()

    split_file(args.input_file, args.output_folder)