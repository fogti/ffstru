const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

export fn add(a: i32, b: i32) i32 {
    return a + b;
}

test "basic add functionality" {
    try testing.expect(add(3, 7) == 10);
}

pub const DocumentReader = struct {
  src: std.fs.StreamSource,

  // cache, mostly to avoid unnecessary syscalls (lseek+read)
  record_cnt: u32,
  indices_start: u32,
  strtab_len: u32,

  const SelfLog = std.log.scoped(.ffstruDocumentReader);
  pub const Self = @This();

  pub fn init(src: std.fs.StreamSource) !Self {
    var ret = Document {
      .src = src,
      .record_cnt = 0,
      .indices_start = 0,
      .strtab_len = 0,
    };
    try ret.reload();
    return ret;
  }

  // re-initialize the index cache
  pub fn reload(self: *Self) !void {
    errdefer {
      self.record_cnt = 0;
      self.indices_start = 0;
      self.strtab_len = 0;
    };
    try self.mySeekTo(0);
    var reader = self.src.reader();
    const bytes = try reader.readBytesNoEof(16);
    if (bytes[0..4] != "yFds")
      return error.InvalidFormat;
    self.record_cnt = std.mem.readIntLittle(u32, bytes[4..8]);
    self.indices_start = std.mem.readIntLittle(u32, bytes[8..12]);
    self.strtab_len = std.mem.readIntLittle(u32, bytes[12..16]);
    if (self.indices_start < (4 + self.strtab_len))
      return error.InvalidFormat;
  }

  fn mySeekTo(self: *Self, offset: u32) !void {
    try self.src.seekTo(@as(u64, offset) * 4);
  }

  // the caller should free the returned string
  pub fn fetchString(self: *Self, allocator: Allocator, offset: u32, max_size: u32) ![]const u8 {
    if (offset < 4 or offset >= (4 + self.strtab_len))
      return error.OutOfBounds;

    try self.mySeekTo(offset);
    var reader = self.src.reader();
    return reader.readUntilDelimiterAlloc(allocator, 0, max_size);
  }

  // non-allocating, more effective alternative to
  // ```
  // const tmp = self.fetchString(allocator, offset, expected.len + 1);
  // defer allocator.free(tmp);
  // std.math.order((tmp[..tmp.len - 1]), expected)) ...
  // ```
  // computes roughly `strtab[offset] [?cmp?] expected`
  // if ==.Less, then the got value was smaller than the expected one.
  pub fn compareString(self: *Self, offset: u32, expected: []const u8) !math.Order {
    if (offset < 4 or offset >= (4 + self.strtab_len))
      return error.OutOfBounds;
    if (expected.len >= (self.strtab_len * 4))
      return .lt;

    try self.mySeekTo(offset);
    var buf_reader = std.io.bufferedReader(self.src.reader());
    var reader = buf_reader.reader();

    var i: usize = 0;
    while (true) : (i += 1) {
      var tmp0: [1]u8 = undefined;
      const amt_read = try reader.read(&tmp0);
      if (amt_read < 1) return .lt;
      const tmp1 = tmp0[0];
      if (i == expected.len) {
        if (tmp1 != 0)
          return .gt;
        return .eq;
      } else {
        const tmpord = std.math.order(tmp1, expected[i]);
        if (tmpord != .eq)
          return tmpord;
      }
    }
  }

  fn handleSeekOverflow(seek_to: *u32, next: u32) error{Overflow}!void {
    if (seek_to.* > (0xffffffff - next)) {
      return error.Overflow;
    } else {
      seek_to.* += next;
    }
  }

  // if successful, returns the offset of the index
  // if just not found, returns EndOfStream
  fn findAttrIndexIntern(self: *Self, seek_to: *u32, attrname: []const u8) !?u32 {
    try self.mySeekTo(seek_to.*);
    var buf_reader = std.io.bufferedReader(self.src.reader());
    var cnt_reader = std.io.countingReader(buf_reader.reader());
    var reader = cnt_reader.reader();
    while (true) {
      const cur_itype = try reader.readIntLittle(u32);
      const cur_next = try reader.readIntLittle(u32);

      if (cur_itype != 0) {
        try Self.handle_seek_overflow(seek_to, cur_next);
        return null;
      }

      const records_cnt = cur_next / 2;
      const indexed_attribute = try reader.readIntLittle(u32);
      const ret = seek_to.* + (cnt_reader.bytes_read / 4) - 4;

      if (try self.compareString(indexed_attribute, attrname) != .Less) {
        // itype, next, _: u32
        try Self.handleSeekOverflow(seek_to, 3);
        try Self.handleSeekOverflow(seek_to, cur_next);
        return null;
      }

      return ret;
    }
  }

  /// if successful, returns the offset of the index
  /// wraps `find_attr_index_intern` to prevent accidentially using corrupted reader structs
  pub fn findAttrIndex(self: *Self, attrname: []const u8) !?u32 {
    var seek_to = self.indices_start.*;
    while (true) {
      if (self.findAttrIndexIntern(&seek_to, itype, attrname)) |tmp| {
        if (tmp) |val| return val;
        // otherwise try again with new seek position
      } else |err| {
        return if (err == .EndOfStream) null else err;
      }
    }
  }

  /// `index_pos` should be determined via `find_attr_index`.
  /// if successful, returns the offset of the record
  pub fn searchAttr(self: *Self, index_pos: u32, key: []const u8) !?u32 {
    if (index_pos < self.indices_start.*)
      return error.OutOfBounds;
    try self.mySeekTo(index_pos);
    var cur_next: u32 = undefined;
    {
      var reader = self.reader();
      const itype = try reader.readIntLittle(u32);
      if (itype != 0)
        return error.InvalidFormat;
      cur_next = try reader.readIntLittle(u32);
    }
    const records_cnt = cur_next / 2;

    var level_start_segment: u32 = 0;
    // each segment consists of 512 key-record-ref pairs
    const segment_size = 1024;
    var sel_segment: u32 = 0;
    var segment_buf = std.BoundedArray(u8, 4 * segment_size).init(0) catch unreachable;
    // each level consists of 513^level_id segments.
    const slevel_base = 513;

    while (true) {
      SelfLog.debug(
          "search_attr: level_start_segment = {d}; sel_segment = {d}",
          .{ level_start_segment, sel_segment });
      if (sel_segment < level_start_segment)
        unreachable;
      if (sel_segment * segment_size >= records_cnt)
        return null;

      // load the entire segment
      {
        segment_data.len = 4096;
        self.mySeekTo(index_pos + 4 + sel_segment * segment_size) catch |err| {
          // if we can't seek at all in the file, then the user
          // has no easy way to get the `index_pos`, so we don't
          // need to consider that case.
          return if (err == .Unseekable) null else err;
        };
        const segment_cutoff = self.src.reader().readAll(segment_buf.slice()) catch |err| {
          return if (err == .EndOfStream) null else err;
        };
        // normally, I would check here if segment_cutoff % 8 == 0, but
        // that might make working with truncated files unnecessary cumbersome.
        segment_buf.resize(segment_cutoff - segment_cutoff % 8) catch unreachable;
      }
      // we don't parse the entire array because it would just trash the cache again,
      // and don't give us any speed benefit.

      // segment is empty, nothing to search
      if (segment_data.len < 8)
        return null;

      // binary search in the segment, it's a sorted list.
      // if this isn't fast enough, we should compare it to sequential search.

      var left: u16 = 0;
      var right: u16 = segment_data.len - 8;
      while (left <= right) {
        const middle = left + ((right - left) / 2);
        if (middle % 8 != 0)
          unreachable;
        const middle_val1 = std.mem.readIntLittle(u32, segment_data[middle..middle + 4]);
        switch (try self.compareString(middle_val1, key)) {
          .eq => {
            // value found
            return std.mem.readIntLittle(u32, segment_data[middle + 4..middle + 8]);
          }
          .lt => {
            // middle < key => continue right
            left = middle + 8;
          }
          .gt => {
            // middle > key => continue left
            if (middle == 0) {
              // prevent underflow
              left = 0;
              right = 0;
              break;
            }
            right = middle - 8;
          }
        }
      }

      // calculate subsegment
      const subsnr = left / 8;
      const next_level_start_segment = (level_start_segment + 1) * slevel_base - 1;
      const prev_offset = sel_segment - level_start_segment;
      sel_segment = next_level_start_segment + slevel_base * prev_offset + subsnr;
      level_start_segment = next_level_start_segment;
    }
  }

  pub fn RecordHandler(
    comptime Context: type,
    comptime RecordError: type,
    // should return true if the attributes should be visited
    comptime headFn: fn (context: Context, number_of_attributes: u32, rtype: u32) RecordError!bool,
    // should return false if browsing should be aborted
    comptime attrFn: fn (context: Context, key: u32, value: u32) RecordError!bool,
  ) type {
    return struct {
      context: Context,
      pub const Error = RecordError;
      const HSelf = @This();

      pub fn head(self: Self, number_of_attributes: u32, rtype: u32) Error!bool {
        return headFn(self.context, number_of_attributes, rtype);
      }

      pub fn attr(self: Self, key: u32, value: u32) Error!bool {
        return attrFn(self.context, key, value);
      }
    };
  }

  pub fn browseRecord(self: *Self, record_offset: u32, handler: anytype) !bool {
    try self.mySeekTo(record_offset);
    var buf_reader = std.io.bufferedReader(self.src.reader());
    var reader = buf_reader.reader();
    const number_of_attributes = try reader.readIntLittle(u32);
    {
      const rtype = try reader.readIntLittle(u32);
      if (!try handler.head(number_of_attributes, rtype))
        return false;
    }
    var attri = 0;
    while (attri < number_of_attributes) : (attri += 1) {
      const key = try reader.readIntLittle(u32);
      const value = try.reader.readIntLittle(u32);
      if (!try handler.attr(key, value))
        return false;
    }
    return true;
  }
};

/// NOTE: all `u32` values in the header are indices, which means that
/// they refer to (val * 4) instead of (val) ; this is an optimization
/// to deal with larger files (up to 16GiB) while still using 32bit indices.
pub const DocumentHeader = {
  record_cnt: u32,
  indices_start: u32,
  strtab_len: u32,

  const Self = @This();

  pub fn writeTo(self: Self, writer: anytype) writer.Error!void {
    try writer.writeIntLittle(u32, self.record_cnt);
    try writer.writeIntLittle(u32, self.indices_start);
    try writer.writeIntLittle(u32, self.strtab_len);
  }
};

/// to achieve optimal compression, it is recommended to first add
/// the longest strings. to speed up insertion, it is recommended
/// to reserve enough capacity beforehand to avoid reallocations.
pub fn appendToStringTable(tab: *std.ArrayList(u8), toapp: []const u8) !u32 {
  // search if the string is already present by first crawling nulls
  for (tab.items) |item, idx| {
    if (item == 0 and idx > toapp.len) {
      const potstart = idx - toapp.len;
      if (std.mem.eql(tab.items[potstart..idx], toapp)) {
        return potstart;
      }
    }
  }
  // not already present
  const ret = tab.items.len;
  try tab.appendSlice(toapp);
  return ret;
}

/// add necessary padding to the string table
pub fn finishStringTable(tab: *std.ArrayList(u8)) !void {
  const padding = tab.items.len % 4;
  var i = 0;
  try tab.ensureUnusedCapacity(3);
  while (i < padding) : (i += 1) {
    (tab.addOneAssumeCapacity().*) = 0;
  }
}

pub const AttrIndexBuilder = {
  dst: std.io.StreamSource,
  start: u64,
  len: u32,

  const Self = @This();

  pub fn init(dst: std.io.StreamSource, indexed_attribute: u32) !Self {
    var writer = dst.writer();
    // itype
    try writer.writeIntLittle(u32, 0);
    // next
    try writer.writeIntLittle(u32, 0);
    // indexed_attribute
    try writer.writeIntLittle(u32, indexed_attribute);
    return Self {
      .dst = dst,
      .start = try dst.getPos(),
      .len = 0,
    };
  }

  pub fn finish(self: Self) !void {
    try self.dst.seekTo(self.start + 4);
    try self.dst.writeIntLittle(u32, self.len);
  }

  pub fn 
};

pub struct AttrIndexKeyValue = struct {
  key: u32,
  value: u32,

  pub fn writeTo(self: Self, writer: anytype) writer.Error!void {
    try writer.writeIntLittle(u32, self.key);
    try writer.writeIntLittle(u32, self.value);
  }
};

const serSegment: [_]AttrIndexKeyValue = [4096 / 8].{
  // sentinel values
  key: 0,
  value: 0,
};

const btreeifyRestrict = enum {
  Left,
  Right,
  
};

fn attrIndexBtreeify(
  segments: []serSegment,
  level_id: u16,
  sel_segment: usize,
  kvps: []AttrIndexKeyValue,
  sgleft: u32,
  sgright: u32,
  
) !void {
  if (kvps.items.len == 0)
    return;
  if (sgleft > sgright)
    unreachable;
  const sgmiddle = sgleft + (sgright - sgleft) / 2;
  const inmiddle = kvps.len / 2;

  segments[sel_segment][sgmiddle] = kvps[inmiddle];

  if (sgleft != sgright) {
    // left branch
    try attrIndexBtreeify(segments, level_id, );
    // right branch
    try attrIndexBtreeify(segments, level_id, );
  } else {
    // next left segment
    try attrIndexBtreeify(segments, level_id + 1, );
  }
}

pub fn serializeAttrIndex(
  allocator: Allocator,
  indexed_attribute: u32,
  // list MUST be ordered 
  kvps: []AttrIndexKeyValue,
  writer: anytype,
) !void {
  // calculate levels and segments
  var segments = std.ArrayList(serSegment).initCapacity(allocator, kvps.items.len / 512);
  defer segments.deinit();

  // perform B+-Tree-ify
  try attrIndexBtreeify(segments.items, 0, 0, kvps, 0, 512);
  // terminating right segment
  try attrIndex

  // write results
  //  exclude unnecessary trailing segments
  var full_segments = segments.len;
  while (full_segments > 0) : (full_segments -= 1) {
    if (segments[full_segments - 1].len != 0)
      break;
  }

  //  itype
  try writer.writeIntLittle(u32, 0);
  //  next
  try writer.writeIntLittle(u32, full_segments * 512);
  //  indexed_attribute
  try writer.writeIntLittle(u32, indexed_attribute);
  //  records_offsets
  for (segments.items) |segment, sgidx| {
    if (sgidx >= full_segments)
      break;
    for (segment.slice()) |sgkvps|
      try sgkvps.writeTo(writer);
  }
}
