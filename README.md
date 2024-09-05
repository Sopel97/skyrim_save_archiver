# Skyrim Save Archiver

Compress skyrim save files utilizing sequential similarity for maximum compression, with the help of [Zstd](https://github.com/facebook/zstd)'s massive window size. 

Made for [Lost Legacy](https://github.com/Lost-Outpost/lost-legacy) but should work with any Skyrim Special Edition playthrough.

## Usage

```
python ess_compress.py --help
python ess_compress.py compress --help
python ess_compress.py decompress --help
```

Using multiple threads does not impact compression quality. Using higher compression levels is recommended for archival, with the highest level (22) still being fast enough to be viable.

## Results

Testing on ~300 saves from Lost Legacy, ~30MB each, shows that it can achieve compression ratios below 10% with respect to original LZ4 compressed saves (default SSE saves), and below 3% for uncompressed saves. 

Generally 5-10 times better than 7-zip with slowest settings, and 2 times better than simple tar -> Zstd without first decompressing the internal save data.

## Behaviour

Compression first gathers full saves (i.e. excludes autosaves and quicksaves) in `.ess` format, and their corresponding `.skse` files if present. 

The files are ordered by the index extracted from the filename. 

The normally internally compressed (LZ4) `.ess` files are decompressed and recompressed using zstd with a large (2GB) window, spanning multiple files. This means only the differences between the saves contributes significantly to the archive size.

`.skse` files are processed after, in the same order, and within the same compressed blob.

On decompression the internal LZ4 compression is redone, with the same settings as SSE uses, to bring the files to full bit-wise equivalence.
