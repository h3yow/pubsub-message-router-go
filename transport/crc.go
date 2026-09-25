package transport

import "hash/crc32"

func crc32IEEE(b []byte) uint32 { return crc32.Checksum(b, crc32.IEEETable) }
