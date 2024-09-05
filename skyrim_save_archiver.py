import lz4.block
import lz4.frame
import zstandard as zstd
import struct
import lz4
from pathlib import Path
import os
from tqdm import tqdm
import io
import argparse

def read_uint64(f) -> int:
    data = f.read(8)
    if len(data) != 8:
        raise Exception('Not enough data for uint64')
    return struct.unpack('<Q', data)[0]

def read_uint32(f) -> int:
    data = f.read(4)
    if len(data) != 4:
        raise Exception('Not enough data for uint32')
    return struct.unpack('<L', data)[0]

def read_uint16(f) -> int:
    data = f.read(2)
    if len(data) != 2:
        raise Exception('Not enough data for uint16')
    return struct.unpack('<H', data)[0]

def read_uint8(f) -> int:
    data = f.read(1)
    if len(data) != 1:
        raise Exception('Not enough data for uint8')
    return struct.unpack('<B', data)[0]

def read_wstring(f) -> bytes:
    size = read_uint16(f)
    data = f.read(size)
    if len(data) != size:
        raise Exception('Not enough data for wstring')
    return data

def pack_uint64(v : int):
    return struct.pack('<Q', v)

def pack_uint32(v : int):
    return struct.pack('<L', v)

def pack_uint16(v : int):
    return struct.pack('<H', v)

def pack_uint8(v : int):
    return struct.pack('<B', v)

def pack_wstring(v : bytes):
    if len(v) > 2**16-1:
        raise Exception('too large for wstring')
    return struct.pack('<H', len(v)) + v


def windows_filetime_to_unix_second(ft):
    # https://learn.microsoft.com/en-us/office/client-developer/outlook/mapi/filetime
    EPOCH_AS_FILETIME = 116444736000000000  # January 1, 1970 as MS file time
    return (ft - EPOCH_AS_FILETIME) // (10**9 // 100)

# https://en.uesp.net/wiki/Skyrim_Mod:Save_File_Format
# Empirically it appears that the whole blob uses LZ4 compression
# and individual "Change Form" objects are not compressed.
# We only read as much as needed, don't parse the whole file.
class ESS:
    def __init__(self, input : io.BufferedIOBase | Path | bytes):
        if isinstance(input, bytes):
            f = io.BytesIO(input)
        elif isinstance(input, Path):
            f = open(input, 'rb')
        elif isinstance(input, io.BufferedIOBase):
            f = input
        else:
            raise Exception(f'Invalid input type: {type(input)}')

        magic = f.read(13)
        if magic != b'TESV_SAVEGAME':
            raise Exception(f'Invalid magic: {magic}')
        
        self.headerSize = read_uint32(f)

        # SE = 12.
        # Maybe support other versions in the future
        self.version = read_uint32(f)
        if self.version != 12:
            raise Exception(f'Unsupported version {self.version}.')

        self.saveNumber = read_uint32(f)
        self.playerName = read_wstring(f)
        self.playerLevel = read_uint32(f)
        self.playerLocation = read_wstring(f)
        self.gameDate = read_wstring(f)
        self.playerRaceEditorId = read_wstring(f)
        self.playerSex = read_uint16(f)
        self.playerCurExp = f.read(4)
        self.playerLvlUpExp = f.read(4)
        self.filetime = read_uint64(f)
        self.shotWidth = read_uint32(f)
        self.shotHeight = read_uint32(f)
        self.compressionType = read_uint16(f)

        self.screenshotData = f.read(4 * self.shotWidth * self.shotHeight)

        if self.compressionType == 0:
            self.uncompressedBlob = f.read()
        elif self.compressionType == 2:
            self.uncompressedLen = read_uint32(f)
            self.compressedLen = read_uint32(f)

            self.uncompressedBlob = lz4.block.decompress(f.read(), uncompressed_size=self.uncompressedLen)

            if len(self.uncompressedBlob) != self.uncompressedLen:
                raise Exception(f'Wrong uncompressed size. Expected {self.uncompressedLen}, got {len(self.uncompressedBlob)}.')
        else:
            raise Exception(f'Invalid compression type {self.compressionType}')
        
        if isinstance(input, Path):
            f.close()

    def get_uncompressed(self):
        chunks = []
        
        chunks.append(b'TESV_SAVEGAME')

        chunks.append(pack_uint32(self.headerSize))

        chunks.append(pack_uint32(self.version))
        chunks.append(pack_uint32(self.saveNumber))
        chunks.append(pack_wstring(self.playerName))
        chunks.append(pack_uint32(self.playerLevel))
        chunks.append(pack_wstring(self.playerLocation))
        chunks.append(pack_wstring(self.gameDate))
        chunks.append(pack_wstring(self.playerRaceEditorId))
        chunks.append(pack_uint16(self.playerSex))
        chunks.append(self.playerCurExp)
        chunks.append(self.playerLvlUpExp)
        chunks.append(pack_uint64(self.filetime))
        chunks.append(pack_uint32(self.shotWidth))
        chunks.append(pack_uint32(self.shotHeight))
        chunks.append(pack_uint16(0)) # no compression

        chunks.append(self.screenshotData)
        # don't put compression sizes in there

        chunks.append(self.uncompressedBlob)

        return b''.join(chunks)
    
    def get_compressed(self):
        chunks = []
        
        chunks.append(b'TESV_SAVEGAME')

        chunks.append(pack_uint32(self.headerSize))

        chunks.append(pack_uint32(self.version))
        chunks.append(pack_uint32(self.saveNumber))
        chunks.append(pack_wstring(self.playerName))
        chunks.append(pack_uint32(self.playerLevel))
        chunks.append(pack_wstring(self.playerLocation))
        chunks.append(pack_wstring(self.gameDate))
        chunks.append(pack_wstring(self.playerRaceEditorId))
        chunks.append(pack_uint16(self.playerSex))
        chunks.append(self.playerCurExp)
        chunks.append(self.playerLvlUpExp)
        chunks.append(pack_uint64(self.filetime))
        chunks.append(pack_uint32(self.shotWidth))
        chunks.append(pack_uint32(self.shotHeight))
        chunks.append(pack_uint16(2)) # LZ4 compression

        chunks.append(self.screenshotData)
        # don't put compression sizes in there
        
        chunks.append(pack_uint32(len(self.uncompressedBlob)))
        compressedBlob = lz4.block.compress(self.uncompressedBlob, store_size=False)
        chunks.append(pack_uint32(len(compressedBlob)))

        chunks.append(compressedBlob)

        return b''.join(chunks)
    
    def get_filetime_as_posix_seconds(self):
        return windows_filetime_to_unix_second(self.filetime)
    
MiB = 1*1024*1024

def get_save_id_from_path(path):
    if not path.name.startswith('Save'):
        raise Exception('Only main saves are supported')
    
    return int(path.name.split('_')[0][4:])

def chunks(arr, size):
    for i in range(0, len(arr), size):
        yield arr[i:i+size]

def append_blob(compressor, blob):
    READ_CHUNK_SIZE = 1 * MiB
    with tqdm(total=len(blob), leave=False, bar_format='{desc}{percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]', unit_scale=True, unit='B') as pbar:
        total_read = 0
        total_write = 0
        total_write_before = compressor.tell()
        for chunk in chunks(blob, size=READ_CHUNK_SIZE):
            total_read += len(chunk)
            compressor.write(chunk)
            #compressor.flush(zstd.FLUSH_BLOCK) # without this progress can be a little off with high compression levels and memory usage can get really high, but prevents efficient multithreading
            total_write = compressor.tell() - total_write_before
            pbar.set_postfix_str(f'ratio {total_write / total_read * 100:0.1f}%')
            pbar.update(len(chunk))

def compress_files(ess_files : list[Path], skse_files : list[Path], output_file : Path, compression_level : int = 3, threads : int = 1):
    params = zstd.ZstdCompressionParameters(enable_ldm=True, compression_level=compression_level, window_log=31, threads=threads)
    cctx = zstd.ZstdCompressor(compression_params=params)
    '''
    format is
        header
            uint32 num_ess_files
            uint32 num_skse_files
        
        entry
            wstring filename
            uint32  filesize
            zstd    packed_data

    .ess file contents are uncompressed before compression
    '''
    with open(output_file, 'wb') as f:
        with cctx.stream_writer(f) as compressor:
            total_read = 0
            total_write = 0

            header = pack_uint32(len(ess_files)) + pack_uint32(len(skse_files))
            append_blob(compressor, header)

            with tqdm(total=len(ess_files), leave=True, bar_format='{desc}{percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]') as pbar:
                pbar.set_description('ESS: ')
                for ess_file in ess_files:
                    total_read += os.path.getsize(ess_file)
                    ess = ESS(ess_file)
                    uncompressed_blob = ess.get_uncompressed()
                    header = pack_wstring(str(ess_file.name).encode(encoding='utf-8')) + pack_uint32(len(uncompressed_blob))
                    append_blob(compressor, header)
                    append_blob(compressor, uncompressed_blob)
                    total_write = compressor.tell()
                    
                    pbar.set_postfix_str(f'ratio {total_write / total_read * 100:0.1f}%')
                    pbar.update(1)

            with tqdm(total=len(skse_files), leave=True, bar_format='{desc}{percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]') as pbar:
                pbar.set_description('SKSE: ')
                for skse_file in skse_files:
                    filesize = os.path.getsize(skse_file)
                    total_read += filesize
                    with open(skse_file, 'rb') as sksef:
                        header = pack_wstring(str(skse_file.name).encode(encoding='utf-8')) + pack_uint32(filesize)
                        append_blob(compressor, header)
                        append_blob(compressor, sksef.read())
                        total_write = compressor.tell()
                        
                    pbar.set_postfix_str(f'ratio {total_write / total_read * 100:0.1f}%')
                    pbar.update(1)



def decompress_files(archive : Path, output_directory : Path):
    '''
    Recompresses .ess files internally
    '''
    
    os.makedirs(output_directory, exist_ok=True)
    
    dctx = zstd.ZstdDecompressor(max_window_size=2**31)
    with open(archive, 'rb') as in_f:
        decompressor = dctx.stream_reader(in_f)
        num_ess_files = read_uint32(decompressor)
        num_skse_files = read_uint32(decompressor)
        stem_to_posix_timestamp = dict()

        with tqdm(total=num_ess_files, leave=True) as pbar:
            pbar.set_description('ESS: ')
            for i in range(num_ess_files):
                name = read_wstring(decompressor).decode(encoding='utf-8')
                output_path = output_directory.joinpath(name)
                if os.path.exists(output_path):
                    raise Exception('Destination exists.')
                filesize = read_uint32(decompressor)
                ess = ESS(decompressor.read(filesize))
                ts = ess.get_filetime_as_posix_seconds()
                stem_to_posix_timestamp[Path(name).stem] = ts

                if not os.path.exists(output_path):
                    with open(output_path, 'wb') as out_f:
                        out_f.write(ess.get_compressed())

                os.utime(output_path, times=(ts, ts))

                pbar.update(1)
                    
        with tqdm(total=num_skse_files, leave=True) as pbar:
            pbar.set_description('SKSE: ')
            for i in range(num_skse_files):
                name = read_wstring(decompressor).decode(encoding='utf-8')
                output_path = output_directory.joinpath(name)
                if os.path.exists(output_path):
                    raise Exception('Destination exists.')
                filesize = read_uint32(decompressor)
                if not os.path.exists(output_path):
                    with open(output_path, 'wb') as out_f:
                        out_f.write(decompressor.read(filesize))

                    ts = stem_to_posix_timestamp[Path(name).stem]
                    os.utime(output_path, times=(ts, ts))
                    
                pbar.update(1)
            
def cli_compress(args):
    ess_files = []
    skse_files = []
    for child in Path.iterdir(args.i):
        # only do full saves
        if not child.name.startswith('Save'):
            continue

        # only get saves that can actually be parsed
        # SSSO3 for example start with "Save"
        try:
            get_save_id_from_path(child)
        except:
            continue

        ext = child.suffix
        if ext == '.ess':
            ess_files.append(child)
            skse_file = Path(str(child)[:-4] + '.skse')
            if os.path.exists(skse_file):
                skse_files.append(skse_file)

    ess_files.sort(key=lambda x: get_save_id_from_path(x))
    skse_files.sort(key=lambda x: get_save_id_from_path(x))
    
    compress_files(ess_files, skse_files, args.o, compression_level=args.c, threads=args.t)

def cli_decompress(args):
    decompress_files(args.i, args.o)

def main():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(required=True)

    parser_foo = subparsers.add_parser('compress')
    parser_foo.add_argument('-i', type=Path, help="Input directory with *.ess and *.skse files.", required=True)
    parser_foo.add_argument('-o', type=Path, help="Output archive path.", required=True)
    parser_foo.add_argument('-c', type=int, default=12, help="Compression level between 1-22. Values higher than 15 may not report progress correctly due to large amounts of buffering. Use 22 for archival")
    parser_foo.add_argument('-t', type=int, default=1, help="Number of threads to use.")
    parser_foo.set_defaults(func=cli_compress)


    parser_bar = subparsers.add_parser('decompress')
    parser_bar.add_argument('-i', type=Path, help="Input archive path.", required=True)
    parser_bar.add_argument('-o', type=Path, help="Output directory path to place the files into. Existing files will be skipped.", required=True)
    parser_bar.set_defaults(func=cli_decompress)

    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()