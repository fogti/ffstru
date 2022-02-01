const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

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

    pub fn head(self: HSelf, number_of_attributes: u32, rtype: u32) Error!bool {
      return headFn(self.context, number_of_attributes, rtype);
    }

    pub fn attr(self: HSelf, key: u32, value: u32) Error!bool {
      return attrFn(self.context, key, value);
    }
  };
}

pub const DocumentReader = struct {
  src: std.io.StreamSource,

  // cache, mostly to avoid unnecessary syscalls (lseek+read)
  record_cnt: u32,
  strtab_len: u32,

  const SelfLog = std.log.scoped(.ffstruDocumentReader);
  pub const Self = @This();
  const headerLen: u32 = 3;

  pub const Error = error {
    AccessDenied,
    EndOfStream,
    InvalidFormat,
    OutOfBounds,
    Overflow,
    Unseekable,
    Unexpected,
  } || std.io.StreamSource.ReadError;

  pub fn init(src: std.io.StreamSource) Error!Self {
    var ret = Self {
      .src = src,
      .record_cnt = 0,
      .strtab_len = 0,
    };
    try ret.reload();
    return ret;
  }

  // re-initialize the index cache
  pub fn reload(self: *Self) Error!void {
    errdefer {
      self.record_cnt = 0;
      self.strtab_len = 0;
    }
    try self.mySeekTo(0);
    var reader = self.src.reader();
    const bytes = try reader.readBytesNoEof(headerLen * 4);
    if (!std.mem.eql(u8, bytes[0..4], "yFds"))
      return error.InvalidFormat;
    self.record_cnt = std.mem.readIntLittle(u32, bytes[4..8]);
    self.strtab_len = std.mem.readIntLittle(u32, bytes[8..12]);
  }

  fn mySeekTo(self: *Self, offset: u32) Error!void {
    try self.src.seekTo(@as(u64, offset) * 4);
  }

  fn indices_start(self: *const Self) u32 {
    // headerLen =#[ magic, record_cnt, strtab_len ]
    return headerLen + 2 * self.record_cnt + self.strtab_len;
  }

  // the caller should free the returned string
  pub fn fetchString(self: *Self, allocator: Allocator, offset: u32, max_size: u32) Error![]const u8 {
    if (offset < headerLen or offset >= (headerLen + self.strtab_len))
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
  pub fn compareString(self: *Self, offset: u32, expected: []const u8) Error!std.math.Order {
    if (offset < headerLen or offset >= (headerLen + self.strtab_len))
      return error.OutOfBounds;
    if (expected.len >= (self.strtab_len * headerLen))
      return std.math.Order.lt;

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

  pub const Ref = union(enum) {
    // 0x0, 0x1
    boolean: bool,
    // >= headerLen
    string: []const u8,
    // >= headerLen + self.strtab_len
    record: u32,

    pub fn deinit(self: Self, allocator: Allocator) void {
      switch (self) {
        .string => |s| allocator.free(s),
        else => {}
      }
    }
  };

  pub fn decodeRef(self: *Self, allocator: Allocator, offset: u32, max_size: u32) Error!Ref {
    return switch (offset) {
      0 => .{ .boolean = false },
      1 => .{ .boolean = true  },
      2 => error.OutOfBounds,
      else =>
        if (offset >= self.indices_start())
          error.OutOfBounds
        else if (offset >= headerLen + self.strtab_len)
          .{ .record = offset }
        else
          self.fetchString(allocator, offset, max_size)
    };
  }

  fn handleSeekOverflow(seek_to: *u32, next: u32) error{Overflow}!void {
    if (seek_to.* > (std.math.maxInt(u32) - next)) {
      return error.Overflow;
    } else {
      seek_to.* += next;
    }
  }

  const attrIndexHeaderLen: u32 = 3;

  // if successful, returns the offset of the index
  // if just not found, returns EndOfStream
  fn findAttrIndexIntern(self: *Self, seek_to: *u32, attrname: []const u8) Error!?u32 {
    try self.mySeekTo(seek_to.*);
    var buf_reader = std.io.bufferedReader(self.src.reader());
    var cnt_reader = std.io.countingReader(buf_reader.reader());
    var reader = cnt_reader.reader();
    while (true) {
      const cur_itype = try reader.readIntLittle(u32);
      const cur_next = try reader.readIntLittle(u32);

      if (cur_itype != 0) {
        try Self.handleSeekOverflow(seek_to, cur_next);
        return null;
      }

      const indexed_attribute = try reader.readIntLittle(u32);
      const ret = seek_to.* + (cnt_reader.bytes_read / 4) - attrIndexHeaderLen;
      if (ret > std.math.maxInt(u32))
        return error.Overflow;

      if ((try self.compareString(indexed_attribute, attrname)) != .eq) {
        try Self.handleSeekOverflow(seek_to, cur_next);
        return null;
      }

      return @intCast(u32, ret);
    }
  }

  /// if successful, returns the offset of the index
  /// wraps `findAttrIndexIntern` to prevent accidentially using corrupted reader structs
  pub fn findAttrIndex(self: *Self, attrname: []const u8) Error!?u32 {
    var seek_to = self.indices_start();
    while (true) {
      if (self.findAttrIndexIntern(&seek_to, attrname)) |tmp| {
        if (tmp) |val| return val;
        // itype, next, _: u32
        try Self.handleSeekOverflow(&seek_to, attrIndexHeaderLen);
        // otherwise try again with new seek position
      } else |err| {
        return if (err == error.EndOfStream) null else err;
      }
    }
  }

  /// `index_pos` should be determined via `findAttrIndex`.
  /// if successful, returns the offset of the record
  pub fn searchAttr(self: *Self, index_pos: u32, key: []const u8) Error!?u32 {
    if (index_pos < self.indices_start())
      return error.OutOfBounds;
    try self.mySeekTo(index_pos);
    // IMPORTANT: we can't use any buffering reader here
    const reader = self.src.reader();
    if ((try reader.readIntLittle(u32)) != @as(u32, 0))
      return error.InvalidFormat;
    const cur_next = try reader.readIntLittle(u32);
    const records_cnt = cur_next / 2;

    // binary search
    const middle_base = index_pos + attrIndexHeaderLen;
    var left: u32 = 0;
    var right: u32 = records_cnt - 1;
    var buf: [8]u8 = undefined;

    while (left <= right) {
      const middle = left + (right - left) / 2;
      try self.mySeekTo(middle_base + middle * 2);
      try reader.readNoEof(&buf);
      const tmp = try self.compareString(std.mem.readIntLittle(u32, buf[0..4]), key);
      switch (tmp) {
        .eq => {
          // value found
          return std.mem.readIntLittle(u32, buf[4..8]);
        },
        .lt => {
          // middle < key => continue right
          left = middle + 1;
        },
        .gt => {
          // middle > key => continue left
          if (middle == 0) {
            // prevent underflow
            break;
          }
          right = middle - 1;
        },
      }
    }

    return null;
  }

  pub fn browseRecord(self: *Self, record_offset: u32, handler: anytype) (Error || @TypeOf(handler).Error)!bool {
    try self.mySeekTo(record_offset);
    var buf_reader = std.io.bufferedReader(self.src.reader());
    var reader = buf_reader.reader();
    const number_of_attributes = try reader.readIntLittle(u32);
    {
      const rtype = try reader.readIntLittle(u32);
      if (!try handler.head(number_of_attributes, rtype))
        return false;
    }
    var attri: u32 = 0;
    while (attri < number_of_attributes) : (attri += 1) {
      const key = try reader.readIntLittle(u32);
      const value = try reader.readIntLittle(u32);
      if (!try handler.attr(key, value))
        return false;
    }
    return true;
  }
};

/// NOTE: all `u32` values in the header are indices, which means that
/// they refer to (val * 4) instead of (val) ; this is an optimization
/// to deal with larger files (up to 16GiB) while still using 32bit indices.
pub const DocumentHeader = struct {
  record_cnt: u32,
  strtab_len: u32,

  // length in 32bit units.
  pub const ilen: u32 = 3;

  pub fn writeTo(self: @This(), writer: anytype) !void {
    try writer.writeAll("yFds");
    try writer.writeIntLittle(u32, self.record_cnt);
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
      if (potstart % 4 == 0 and std.mem.eql(u8, tab.items[potstart..idx], toapp)) {
        const ret = potstart / 4;
        if (ret > @as(usize, std.math.maxInt(u32)))
          unreachable;
        return @intCast(u32, ret);
      }
    }
  }
  if (tab.items.len % 4 != 0) {
    std.debug.print("tab.items.len = {d}\n", .{ tab.items.len });
    @panic("string table with invalid padding");
  }
  if (tab.items.len + toapp.len > 4 * @as(usize, std.math.maxInt(u32)))
    return error.Overflow;
  const ret = @intCast(u32, tab.items.len / 4);
  const padding = 5 - (toapp.len + 1) % 4;
  try tab.ensureUnusedCapacity(toapp.len + padding);
  try tab.appendSlice(toapp);
  var i: usize = 0;
  while (i < padding) : (i += 1) {
    tab.addOneAssumeCapacity().* = 0;
  }
  return DocumentHeader.ilen + ret;
}

pub const AttrIndexKeyValue = struct {
  key: u32,
  value: u32,

  pub fn writeTo(self: @This(), writer: anytype) !void {
    try writer.writeIntLittle(u32, self.key);
    try writer.writeIntLittle(u32, self.value);
  }
};

pub fn serializeAttrIndex(
  indexed_attribute: u32,
  // list MUST be ordered 
  kvps: []AttrIndexKeyValue,
  writer: anytype,
) !void {
  //  itype
  try writer.writeIntLittle(u32, 0);
  //  next
  if (kvps.len > @as(usize, std.math.maxInt(u32)) / 2)
    return error.Overflow;
  try writer.writeIntLittle(u32, @intCast(u32, kvps.len * 2));
  //  indexed_attribute
  try writer.writeIntLittle(u32, indexed_attribute);
  //  records_offsets
  for (kvps) |kvp| try kvp.writeTo(writer);
}

const test_allocator = std.testing.allocator;
const test_expect = std.testing.expectEqual;

test "simple" {
  var buffer: [4096]u8 = undefined;
  var bufstream = std.io.StreamSource { .buffer = std.io.fixedBufferStream(&buffer) };

  const BaseDate = struct {
    key: []const u8,
    value: []const u8,
  };

  const basedata = [_]BaseDate{
    .{ .key = "hello", .value = "world" },
  };

  // construct a simple document
  var aid: u32 = undefined;
  {
    var strtab = std.ArrayList(u8).init(test_allocator);
    defer strtab.deinit();
    var recs = std.ArrayList(AttrIndexKeyValue).init(test_allocator);
    defer recs.deinit();

    aid = try appendToStringTable(&strtab, "attr 4 index");
    for (basedata) |bd| {
      const k = try appendToStringTable(&strtab, bd.key);
      const v = try appendToStringTable(&strtab, bd.value);
      (try recs.addOne()).* = .{ .key = k, .value = v };
    }
    try test_expect(strtab.items.len, 4 * 8);

    const writer = bufstream.writer();
    try (DocumentHeader {
      .record_cnt = 2,
      .strtab_len = @intCast(u32, strtab.items.len / 4),
    }).writeTo(writer);
    try writer.writeAll(strtab.items);

    try writer.writeIntLittle(u32, 1);
    try writer.writeIntLittle(u32, 0);
    for (recs.items) |rec| try rec.writeTo(writer);

    try serializeAttrIndex(aid, recs.items, writer);
  }
  bufstream.seekTo(0) catch unreachable;

  // check reading/deserializing document
  var docread = try DocumentReader.init(bufstream);

  // test strings
  try test_expect(@as(anyerror!std.math.Order, .eq), docread.compareString(aid, "attr 4 index"));

  // test index capabilities
  const a4ii_ = try docread.findAttrIndex("attr 4 index");
  try test_expect(@as(?u32, 15), a4ii_);
  const a4ii = a4ii_.?;
  try test_expect(@as(?u32, null), try docread.searchAttr(a4ii, "unknown"));
  try test_expect(@as(?u32, 9), try docread.searchAttr(a4ii, "hello"));

  // test record stuff
  const Rech = struct {
    calls: *u32,
    pub const Error = anyerror;
    const HSelf = @This();

    pub fn head(self: HSelf, number_of_attributes: u32, rtype: u32) Error!bool {
      self.calls.* += 0xf00;
      try test_expect(number_of_attributes, 1);
      try test_expect(rtype, 0);
      return true;
    }

    pub fn attr(self: HSelf, key: u32, value: u32) Error!bool {
      self.calls.* += 0x00f;
      try test_expect(@as(u32, 7), key);
      try test_expect(@as(u32, 9), value);
      return true;
    }
  };

  var rech_calls: u32 = 0;
  const rech = Rech {
    .calls = &rech_calls,
  };
  _ = try docread.browseRecord(3 + 8, rech);
  try test_expect(rech_calls, 0xf0f);
}
