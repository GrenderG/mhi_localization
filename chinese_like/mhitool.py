import argparse
import os
import struct
import traceback

def unpack(input_file, output_folder):
    try:
        if not os.path.isfile(input_file):
            raise ValueError("输入文件不存在")
        
        file_size = os.path.getsize(input_file)
        if file_size < 4:
            raise ValueError("输入文件过小")
        
        with open(input_file, 'rb') as f:
            i_byte = f.read(1)
            if not i_byte:
                raise ValueError("文件头损坏")
            i = ord(i_byte)

            if i == 0 or file_size <= (i * 2 + 1):
                raise ValueError(f"无效文件头（i={i}，文件大小={file_size}）")

            unpack_list = []
            total_blocks_size = 0
            for _ in range(i):
                block_bytes = f.read(2)
                if len(block_bytes) != 2:
                    raise ValueError("块大小信息不完整")
                
                (block_size,) = struct.unpack('<H', block_bytes)
                
                if block_size == 0:
                    raise ValueError("块大小为零")
                
                unpack_list.append(block_size)
                total_blocks_size += block_size

            header_size = 1 + i * 2
            expected_size = header_size + total_blocks_size
            if expected_size > file_size:
                raise ValueError(f"大小校验失败：预期{expected_size}字节，实际{file_size}字节")

            os.makedirs(output_folder, exist_ok=True)
            
            for idx, size in enumerate(unpack_list):
                filename = f"{idx:03d}.dat"
                output_path = os.path.join(output_folder, filename)
                
                data = f.read(size)
                if len(data) != size:
                    raise ValueError(f"{filename} 数据不完整，预期{size}字节，实际{len(data)}字节")
                
                with open(output_path, 'wb') as out_file:
                    out_file.write(data)

            if remaining := f.read():
                print(f"警告：输入文件存在{len(remaining)}字节冗余数据")

        print("解包成功")

    except Exception as e:
        print(f"[解包错误] {str(e)}\n追踪：{traceback.format_exc()}")
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
            raise ValueError("未找到有效.dat文件")
        
        dat_files.sort(key=lambda x: x)
        i = len(dat_files)
        if i > 255:
            raise ValueError(f"文件数量超过限制（{i}/255）")

        repack_sizes = []
        total_data = 0
        for index, fname in dat_files:
            file_path = os.path.join(input_folder, fname)
            size = os.path.getsize(file_path)
            if size == 0 or size > 0xFFFF:
                raise ValueError(f"无效文件大小：{fname} ({size}字节)")
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
            raise ValueError(f"打包校验失败：预期{expected_size}字节，实际{actual}字节")

        print("打包成功")

    except Exception as e:
        print(f"[打包错误] {str(e)}\n追踪：{traceback.format_exc()}")
        exit(1)

def analyze(input_file, output_file):

    try:
        with open(args.output_file, 'w') as f:
            pass

        with open(args.input_file, 'rb') as f_in:
            p_byte = f_in.read(1)
            if not p_byte:
                raise ValueError("无效的文件格式: 文件为空")
            
            p = ord(p_byte)
            if p == 0:
                raise ValueError("无效的文件格式: 首字节为 0x00")

            data = f_in.read(2 * p)
            if len(data) != 2 * p:
                raise ValueError("无效的文件格式: 文件头不完整")

            A = []
            B = []
            C = 0
            for i in range(p):
                a = ord(data[2*i:2*i+1])
                b = ord(data[2*i+1:2*i+2])
                
                if a > 2:
                    raise ValueError(f"无效的文件格式: A{i+1}的值{a}大于2")
                if b == 0:
                    raise ValueError(f"无效的文件格式: B{i+1}的值为0")
                
                A.append(a)
                B.append(b)
                C += b

            f_in.seek(0, os.SEEK_END)
            T = f_in.tell()
            remaining = T - (2 * p + 1)
            
            if remaining <= 0 or remaining % C != 0:
                raise ValueError("无效的文件格式: 文件大小不符合要求")

            E = [f"{A[i]},{B[i]}" for i in range(p)]
            
            with open(args.output_file, 'w') as f_out:
                f_out.write("\t".join(E) + "\n")

            f_in.seek(2 * p + 1)
            iterations = remaining // C
            
            with open(args.output_file, 'a') as f_out:
                for _ in range(iterations):
                    S = []
                    for b in B:
                        chunk = f_in.read(b)
                        if len(chunk) != b:
                            with open(args.output_file, 'w') as f:
                                pass
                            raise ValueError("无效的文件格式: 数据不完整")
                        
                        hex_str = "".join([f"{byte:02X}" for byte in chunk])
                        S.append(hex_str)
                    
                    f_out.write("\t".join(S) + "\n")
        print("MHi通用文件格式解析完毕")
    except Exception as e:
        print(f"错误: {str(e)}")
        with open(args.output_file, 'w') as f:
            pass
        return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="MHi文件打包解包与分析工具 (v1.2)")
    subparsers = parser.add_subparsers(dest='command', required=True)

    unpack_parser = subparsers.add_parser('unpack', 
        help='解包MHi包文件到指定文件夹',
        description='尝试将MHi使用的包文件解包到指定目录')
    unpack_parser.add_argument('input_file', help='输入的MHi包文件路径')
    unpack_parser.add_argument('output_folder', help='解包输出的目标文件夹路径')

    repack_parser = subparsers.add_parser('repack',
        help='将文件夹重新打包为MHi包文件',
        description='待打包文件名需使用数字, 后缀为.dat, 将按照数字顺序进行打包')
    repack_parser.add_argument('input_folder', help='包含待打包文件的文件夹路径')
    repack_parser.add_argument('output_file', help='生成的MHi包文件路径')

    analyze_parser = subparsers.add_parser('analyze',
        help='分析MHi通用文件并输出',
        description='尝试根据MHi通用文件(非包文件)结构信息，分析并生成拆分相关数据的文本文件')
    analyze_parser.add_argument('input_file', help='待分析的MHi通用文件路径')
    analyze_parser.add_argument('output_file', help='分析文件输出文件路径')

    args = parser.parse_args()

    try:
        if args.command == 'unpack':
            unpack(args.input_file, args.output_folder)
        elif args.command == 'repack':
            repack(args.input_folder, args.output_file)
        elif args.command == 'analyze':
            analyze(args.input_file, args.output_file)
    except Exception as e:
        print(f"程序终止：{str(e)}\n{traceback.format_exc()}")
        exit(1)
