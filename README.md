# ffstru

## precedents
- GHR record format
- ZTX document format
- dbtin dump format
- tgs spec format
- packlist format
- Fefe tinyldap primary format

## primary data structure
- read-optimized
- partial column-orientied, store data according to access needs
- align data at 4 bytes
- all integers are stores as little endian
- indices can omit the last 2 bits, because they are always zero
- max size: a file/document can be at most 16GiB big, and thus contain
  2^32 32bit integers

(for usage examples, look at the tests in `src/main.zig`)

```zig
magic: u32 = 0x73644679 // "yFds" little-endian
record_cnt: u32, // = sizeof(records) / (2 * sizeof(u32))
// this is an index, so it measures length in 32bit units.
strtab_len: u32,
// the string table gets padded to 4 bytes,
// filled up with zeros at the end
strtab: [strtab_len * 4]u8,

records: []Record,
indices: [.implicit]Index,

pub const Record = struct {
  number_of_attributes: u32,
  rtype: u32,
  attrs: [number_of_attributes][2]u32,
};

pub const Index = struct {
  itype: u32,

  // offset of next index
  // next = (sizeof(self) / sizeof(u32)) - 3
  next: u32,

  // : if itype == 0 :
  indexed_attribute: u32,
  // records should be sorted by lexicographic order
  records_offsets: [records_cnt][2]u32,
};
```
