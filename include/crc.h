#ifndef CRCS_16_64_
#define CRCS_16_64_

// Method calculating an CRC of 16 bits from a string.
uint16_t crc16(const char *buf, int len);

// Method calculating an CRC of 64 bits from a string.
uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);

#endif
