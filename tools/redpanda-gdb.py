#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# redpanda is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# redpanda is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with redpanda.  If not, see <http://www.gnu.org/licenses/>.
#
#   - Copyright (C) 2015 redpandaDB
#   - Copyright 2020 Redpanda Data, Inc. - libc++ support and redpanda types
#
# -----
#
# (gdb) source redpanda-gdb.py
# (gdb) redpanda memory
#
import argparse
import bisect
import collections
import functools
import io
import random
import re
import struct
import sys
from collections import Counter, defaultdict
from enum import Enum
from io import BufferedReader
from operator import attrgetter

import gdb
import gdb.printing

SERDE_ENVELOPE_FORMAT = "<BBI"
SERDE_ENVELOPE_SIZE = struct.calcsize(SERDE_ENVELOPE_FORMAT)

SerdeEnvelope = collections.namedtuple(
    "SerdeEnvelope", ("version", "compat_version", "size")
)


# TODO: export as an external python module to use in gdb script and offline log viewer
class Endianness(Enum):
    BIG_ENDIAN = 0
    LITTLE_ENDIAN = 1


class Reader:
    def __init__(self, stream, endianness=Endianness.LITTLE_ENDIAN):
        # BytesIO provides .getBuffer(), BufferedReader peek()
        self.stream = BufferedReader(stream)
        self.endianness = endianness

    @staticmethod
    def _decode_zig_zag(v):
        return (v >> 1) ^ (~(v & 1) + 1)

    def read_varint(self):
        shift = 0
        result = 0
        while True:
            i = ord(self.stream.read(1))
            if i & 128:
                result |= (i & 0x7F) << shift
            else:
                result |= i << shift
                break
            shift += 7

        return Reader._decode_zig_zag(result)

    def with_endianness(self, str):
        ch = "<" if self.endianness == Endianness.LITTLE_ENDIAN else ">"
        return f"{ch}{str}"

    def read_int8(self):
        return struct.unpack(self.with_endianness("b"), self.stream.read(1))[0]

    def read_uint8(self):
        return struct.unpack(self.with_endianness("B"), self.stream.read(1))[0]

    def read_int16(self):
        return struct.unpack(self.with_endianness("h"), self.stream.read(2))[0]

    def read_uint16(self):
        return struct.unpack(self.with_endianness("H"), self.stream.read(2))[0]

    def read_int32(self):
        return struct.unpack(self.with_endianness("i"), self.stream.read(4))[0]

    def read_uint32(self):
        return struct.unpack(self.with_endianness("I"), self.stream.read(4))[0]

    def read_int64(self):
        return struct.unpack(self.with_endianness("q"), self.stream.read(8))[0]

    def read_uint64(self):
        return struct.unpack(self.with_endianness("Q"), self.stream.read(8))[0]

    def read_serde_enum(self):
        return self.read_int32()

    def read_iobuf(self):
        len = self.read_int32()
        return self.stream.read(len)

    def read_bool(self):
        return self.read_int8() == 1

    def read_string(self):
        len = self.read_int32()
        return self.stream.read(len).decode("utf-8")

    def read_kafka_string(self):
        len = self.read_int16()
        return self.stream.read(len).decode("utf-8")

    def read_kafka_bytes(self):
        len = self.read_int32()
        return self.stream.read(len)

    def read_optional(self, type_read):
        present = self.read_int8()
        if present == 0:
            return None
        return type_read(self)

    def read_kafka_optional_string(self):
        len = self.read_int16()
        if len == -1:
            return None
        return self.stream.read(len).decode("utf-8")

    def read_vector(self, type_read):
        sz = self.read_int32()
        ret = []
        for i in range(0, sz):
            ret.append(type_read(self))
        return ret

    def read_envelope(self, type_read=None, max_version=0):
        header = self.read_bytes(SERDE_ENVELOPE_SIZE)
        envelope = SerdeEnvelope(*struct.unpack(SERDE_ENVELOPE_FORMAT, header))
        if type_read is not None:
            if envelope.version <= max_version:
                return {"envelope": envelope} | type_read(self, envelope.version)
            else:
                return {
                    "error": {
                        "max_supported_version": max_version,
                        "envelope": envelope,
                    }
                }
        return envelope

    def read_serde_vector(self, type_read):
        sz = self.read_uint32()
        ret = []
        for i in range(0, sz):
            ret.append(type_read(self))
        return ret

    def read_tristate(self, type_read):
        state = self.read_int8()
        t = {}
        if state == -1:
            t["state"] = "disabled"
        elif state == 0:
            t["state"] = "empty"
        else:
            t["value"] = type_read(self)
        return t

    def read_bytes(self, length):
        return self.stream.read(length)

    def peek(self, length):
        return self.stream.peek(length)

    def read_uuid(self):
        return "".join(
            [
                f"{self.read_uint8():02x}" + ("-" if k in [3, 5, 7, 9] else "")
                for k in range(16)
            ]
        )

    def peek_int8(self):
        # peek returns the whole memory buffer, slice is needed to conform to struct format string
        return struct.unpack("<b", self.stream.peek(1)[:1])[0]

    def skip(self, length):
        self.stream.read(length)

    def remaining(self):
        return len(self.stream.raw.getbuffer()) - self.stream.tell()

    def read_serde_map(self, k_reader, v_reader):
        ret = {}
        for _ in range(self.read_uint32()):
            key = k_reader(self)
            val = v_reader(self)
            ret[key] = val
        return ret


class std_unique_ptr:
    def __init__(self, obj):
        self.obj = obj

    def get(self):
        return self.obj["__ptr_"]["__value_"]

    def dereference(self):
        return self.get().dereference()

    def __getitem__(self, item):
        return self.dereference()[item]

    def address(self):
        return self.get()

    def __nonzero__(self):
        return bool(self.get())

    def __bool__(self):
        return self.__nonzero__()


class std_optional:
    def __init__(self, ref):
        self.ref = ref

    def get(self):
        assert self.__bool__()
        try:
            return self.ref["__val_"]
        except gdb.error:
            return self.ref["__value_"].dereference()

    def __bool__(self):
        try:
            return bool(self.ref["__engaged_"])
        except gdb.error:
            return bool(self.ref["__value_"])


class chunked_vector:
    def __init__(self, ref):
        self.ref = ref

        self.container_type = self.ref.type.strip_typedefs()
        self.element_type = self.container_type.template_argument(0)
        self.element_size_bytes = self.element_type.sizeof

    def size_bytes_capacity(self):
        return self.capacity() * self.element_size_bytes

    def __len__(self):
        return int(self.ref["_size"])

    def capacity(self):
        return int(self.ref["_capacity"])

    def size_bytes(self):
        return len(self) * self.element_size_bytes


class std_vector:
    def __init__(self, ref):
        self.ref = ref

        self.container_type = self.ref.type.strip_typedefs()
        self.element_type = self.container_type.template_argument(0)
        self.element_size_bytes = self.element_type.sizeof

        end_cap_type = self.ref["__end_cap_"].type.strip_typedefs()

        end_cap_type = end_cap_type.template_argument(0)
        end_cap_type_fmt = "std::__1::__compressed_pair_elem<{}, 0, false>"
        try:
            self.end_cap_type = gdb.lookup_type(end_cap_type_fmt.format(end_cap_type))
        except:
            # Try converting "struct foo *" into "foo*": sometimes GDB reports the type
            # one way, but expects us to give it the other way
            # For example:
            #   std::__1::__compressed_pair_elem<seastar::shared_object*, 0, false>)
            s = str(end_cap_type)
            m = re.match("struct ([\\w:]+) \\*", s)
            if m:
                self.end_cap_type = gdb.lookup_type(
                    end_cap_type_fmt.format(m.group(1) + "*")
                )
            else:
                raise

    def size_bytes(self):
        return len(self) * self.element_size_bytes

    def size_bytes_capacity(self):
        # TODO: this is more direct
        # end_cap = self.ref["__end_cap_"].address
        # end_cap = end_cap.cast(self.end_cap_type.pointer())["__value_"]
        # allocated = int(end_cap) - int(self.ref["__begin_"])
        return self.capacity() * self.element_size_bytes

    def capacity(self):
        end_cap = self.ref["__end_cap_"].address
        end_cap = end_cap.cast(self.end_cap_type.pointer())["__value_"]
        return end_cap - self.ref["__begin_"]

    def __len__(self):
        return int(self.ref["__end_"] - self.ref["__begin_"])

    def __iter__(self):
        i = self.ref["__begin_"]
        end = self.ref["__end_"]
        while i != end:
            yield i.dereference()
            i += 1

    def __getitem__(self, item):
        return (self.ref["__begin_"] + item).dereference()

    def __nonzero__(self):
        return self.__len__() > 0

    def __bool__(self):
        return self.__nonzero__()


class absl_btree_map_params:
    def __init__(self, params_type):
        self.kt = params_type.template_argument(0)
        self.vt = params_type.template_argument(1)
        self.cmp = params_type.template_argument(2)
        self.alloc_t = params_type.template_argument(3)
        self.target_node_sz = params_type.template_argument(4)
        self.is_multi = params_type.template_argument(5)


class absl_layout:
    def __init__(self, type, *args):
        self.types = []
        alignments = []
        self.sizes = args
        for i in range(4):
            tmpl = type.template_argument(i).strip_typedefs()
            self.types.append(tmpl)
            alignments.append(tmpl.alignof)

        self.alignment = max(alignments)

    def element_index(self, type):
        for i, t in enumerate(self.types):
            if t == type:
                return i

    def align(self, a, b):
        return (a + b - 1) & ~(b - 1)

    def offset(self, type_idx):
        if type_idx == 0:
            return 0
        return self.align(
            self.offset(type_idx - 1)
            + self.types[type_idx - 1].sizeof * self.sizes[type_idx - 1],
            self.types[type_idx].alignof,
        )

    def pointer(self, type, base_ptr):
        ptr = base_ptr + self.offset(self.element_index(type))
        ptr_type = type.pointer().strip_typedefs()
        ptr_v = gdb.parse_and_eval(f"({ptr_type}){ptr}")
        return ptr_v


class absl_btree_map_node:
    def __init__(self, ref: gdb.Value):
        self.type = ref.type.strip_typedefs()
        self.ref = ref
        self.layout_type = gdb.lookup_type(f"{self.type}::layout_type").strip_typedefs()
        self.slots = gdb.parse_and_eval(f"(int){self.type}::kNodeSlots")
        self.params = absl_btree_map_params(self.type.template_argument(0))
        self.internal_layout = absl_layout(
            self.layout_type, 1, 0, 4, self.slots, self.slots + 1
        )
        self.leaf_layout = absl_layout(self.layout_type, 1, 0, 4, self.slots, 0)

    def get_field(self, idx):
        tp = self.internal_layout.types[idx]

        return self.internal_layout.pointer(tp, self.ref.address)

    def parent(self):
        return absl_btree_map_node(self.get_field(0).dereference().dereference())

    def slot(self, idx):
        return self.get_field(3)[idx]

    def finish(self):
        return self.get_field(2)[2]

    def is_leaf(self):
        return self.get_field(2)[3] != 0

    def position(self):
        return self.get_field(2)[0]

    def start(self):
        return 0

    def is_root(self):
        return self.parent().is_leaf()

    def child(self, idx):
        return absl_btree_map_node(self.get_field(4)[idx].dereference().dereference())

    def is_internal(self):
        return not self.is_leaf()

    def start_child(self):
        return self.child(self.start())


class absl_btree_map:
    def __init__(self, ref):
        self.ref = ref
        container_type = self.ref.type.strip_typedefs()
        self.tree = ref["tree_"]
        self.tree_type = self.tree.type
        self.kt = container_type.template_argument(0)
        self.vt = container_type.template_argument(1)

        self.root = absl_btree_map_node(self.tree["root_"].dereference())
        self.rightmost_node = absl_btree_map_node(
            self.tree["rightmost_"]["value"].dereference()
        )

        # iterator part
        self.node_it = self.leftmost()
        self.pos_it = self.leftmost().start()

    def leftmost(self):
        return self.root.parent()

    def __iter__(self):
        while True:
            value = self.node_it.slot(self.pos_it)["value"]
            yield value["first"], value["second"]

            # node_->is_leaf() && ++position_ < node_->finish()
            self.pos_it += 1
            if (
                self.node_it.ref.address == self.rightmost_node.ref.address
                and self.pos_it == self.node_it.finish()
            ):
                break

            if self.node_it.is_leaf() and self.pos_it < self.node_it.finish():
                # already incremented position iterator
                continue
            # increment_slow
            if self.node_it.is_leaf():
                while (
                    self.pos_it == self.node_it.finish() and not self.node_it.is_root()
                ):
                    self.pos_it = self.node_it.position()
                    self.node_it = self.node_it.parent()
            else:
                self.node_it = self.node_it.child(self.pos_it + 1)
                while self.node_it.is_internal():
                    self.node_it = self.node_it.start_child()
                self.pos_it = self.node_it.start()

    def size(self):
        # the size is the number of elements in the tree. absl also tracks the
        # capacity of the tree since tree nodes may not all be full. when
        # investigating memory usage, this could be an important difference.
        return self.ref["tree_"]["size_"]


def print_fields(value):
    print(
        f"# Type {value.type.name} has {len(value.type.strip_typedefs().fields())} fields"
    )
    for i, field in enumerate(value.type.fields()):
        print(
            f"# Field: {i} bc: {field.is_base_class} name: {field.name} == {value[field]}"
        )


def get_base_class(value):
    value.type.fields()
    for field in value.type.fields():
        if field.is_base_class:
            return value[field]
    return None


def absl_insert_version_after_absl(cpp_name):
    """Insert version inline namespace after the first `absl` namespace found in the given string."""
    # See more:
    # https://github.com/abseil/abseil-cpp/blob/929c17cf481222c35ff1652498994871120e832a/absl/base/options.h#L203
    ABSL_OPTION_INLINE_NAMESPACE_NAME = "lts_20230802"

    absl_ns_str = "absl::"
    absl_ns_start = cpp_name.find(absl_ns_str)
    if absl_ns_start == -1:
        raise ValueError("No `absl` namespace found in " + cpp_name)

    absl_ns_end = absl_ns_start + len(absl_ns_str)

    return (
        cpp_name[:absl_ns_end]
        + ABSL_OPTION_INLINE_NAMESPACE_NAME
        + "::"
        + cpp_name[absl_ns_end:]
    )


def absl_container_size(settings):
    return settings["compressed_tuple_"]["value"]


def absl_get_settings(val):
    """Gets the settings_ field for abseil (flat/node)_hash_(map/set)."""
    return val["settings_"]["value"]


MAIN_GLOBAL_BLOCK = None


def lookup_type(gdb_type_str: str) -> gdb.Type:
    """
    Try to find the type object from string.

    GDB says it searches the global blocks, however this appear not to be the
    case or at least it doesn't search all global blocks, sometimes it required
    to get the global block based off the current frame.
    """
    global MAIN_GLOBAL_BLOCK

    exceptions = []
    try:
        return gdb.lookup_type(gdb_type_str)
    except Exception as exc:
        exceptions.append(exc)

    if MAIN_GLOBAL_BLOCK is None:
        MAIN_GLOBAL_BLOCK = gdb.lookup_symbol("main")[0].symtab.global_block()

    try:
        return gdb.lookup_type(gdb_type_str, MAIN_GLOBAL_BLOCK)
    except Exception as exc:
        exceptions.append(exc)

    raise gdb.error(
        "Failed to get type, tried:\n%s" % "\n".join([str(exc) for exc in exceptions])
    )


def absl_get_nodes(val):
    """Return a generator of every node in absl::container_internal::raw_hash_set and derived classes."""
    settings = absl_get_settings(val)
    size = absl_container_size(settings)
    if size == 0:
        return

    capacity = int(settings["capacity_"])
    ctrl = settings["control_"]

    # Derive the underlying type stored in the container.
    slot_type = lookup_type(
        str(val.type.strip_typedefs()) + "::slot_type"
    ).strip_typedefs()
    # Using the array of ctrl bytes, search for in-use slots and return them
    # https://github.com/abseil/abseil-cpp/blob/8a3caf7dea955b513a6c1b572a2423c6b4213402/absl/container/internal/raw_hash_set.h#L2108-L2113
    for item in range(capacity):
        ctrl_t = int(ctrl[item])
        if ctrl_t >= 0:
            yield settings["slots_"].cast(slot_type.pointer())[item]


class absl_flat_hash_map:
    """
    This is based off of absl::lts_20230802. This can be inspected dynamically
    by looking at the container type derived in the constructor and then we can
    use that to dynamically select different implementations when it comes time
    to revise this implementation for a newer version of abseil.
    """

    def __init__(self, p):
        self.map = p
        self.container_type = self.map.type.strip_typedefs()
        self.kt = self.container_type.template_argument(0)
        self.vt = self.container_type.template_argument(1)
        self.settings = self.map["settings_"]["value"]

    def capacity(self):
        return self.settings["capacity_"]

    def __len__(self):
        return self.settings["compressed_tuple_"]["value"]

    def __iter__(self):
        slot_type = lookup_type(f"{self.container_type}::slot_type").strip_typedefs()
        control = self.settings["control_"]
        slots = self.settings["slots_"].cast(slot_type.pointer())

        for i in range(self.capacity()):
            if int(control[i]) < 0:
                continue
            slot = slots[i]
            yield slot["value"]["first"], slot["value"]["second"]


def has_enable_lw_shared_from_this(type):
    for f in type.fields():
        if f.is_base_class and "enable_lw_shared_from_this" in f.name:
            return True
    return False


def remove_prefix(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix) :]
    return s


class seastar_lw_shared_ptr:
    def __init__(self, ref):
        self.ref = ref
        self.elem_type = ref.type.template_argument(0)

    def _no_esft_type(self):
        try:
            return gdb.lookup_type(
                "seastar::lw_shared_ptr_no_esft<%s>"
                % remove_prefix(str(self.elem_type.unqualified()), "class ")
            ).pointer()
        except:
            return gdb.lookup_type(
                "seastar::shared_ptr_no_esft<%s>"
                % remove_prefix(str(self.elem_type.unqualified()), "class ")
            ).pointer()

    def get(self):
        if has_enable_lw_shared_from_this(self.elem_type):
            return self.ref["_p"].cast(self.elem_type.pointer())
        else:
            return self.ref["_p"].cast(self._no_esft_type())["_value"].address


class seastar_promise:
    def __init__(self, ref):
        self.ref = ref
        self._state = ref["_state"].dereference()
        self._task = ref["_task"].dereference()

    def __repr__(self):
        return f"promise(state={self._state}, task_ptr={self.ref['_task']} task={self._task})"


class seastar_basic_rwlock:
    def __init__(self, ref):
        self.ref = ref
        self.count = ref["_sem"]["_count"]
        self.wait_list = abortable_fifo(ref["_sem"]["_wait_list"])
        self.wait_list_nrs = [
            int(std_optional(e.payload).get()["nr"]) for e in self.wait_list
        ]
        self.wait_list_prs = [
            seastar_promise(std_optional(e.payload).get()["pr"]) for e in self.wait_list
        ]

    def __repr__(self):
        return f"basic_rwlock(count={self.count}, wait_list_size={self.wait_list.size}, wait_list_nrs={self.wait_list_nrs}, wait_list_prs={self.wait_list_prs})"


class seastar_shared_ptr:
    def __init__(self, ref):
        self.ref = ref

    def get(self):
        return self.ref["_p"]


class seastar_sstring:
    def __init__(self, ref):
        self.ref = ref

    def __len__(self):
        if self.ref["u"]["internal"]["size"] >= 0:
            return int(self.ref["u"]["internal"]["size"])
        else:
            return int(self.ref["u"]["external"]["size"])


class seastar_circular_buffer(object):
    def __init__(self, ref):
        self.ref = ref

    def _mask(self, i):
        return i & (int(self.ref["_impl"]["capacity"]) - 1)

    def __iter__(self):
        impl = self.ref["_impl"]
        st = impl["storage"]
        cap = impl["capacity"]
        i = impl["begin"]
        end = impl["end"]
        while i < end:
            yield st[self._mask(i)]
            i += 1

    def size(self):
        impl = self.ref["_impl"]
        return int(impl["end"]) - int(impl["begin"])

    def __len__(self):
        return self.size()

    def __getitem__(self, item):
        impl = self.ref["_impl"]
        return (impl["storage"] + self._mask(int(impl["begin"]) + item)).dereference()

    def external_memory_footprint(self):
        impl = self.ref["_impl"]
        return int(impl["capacity"]) * self.ref.type.template_argument(0).sizeof


class seastar_static_vector:
    def __init__(self, ref):
        self.ref = ref

    def __len__(self):
        return int(self.ref["m_holder"]["m_size"])

    def __iter__(self):
        t = self.ref.type.strip_typedefs()
        value_type = t.template_argument(0)
        try:
            data = self.ref["m_holder"]["storage"]["data"].cast(value_type.pointer())
        except:
            try:
                data = self.ref["m_holder"]["storage"]["dummy"]["dummy"].cast(
                    value_type.pointer()
                )  # redpanda 3.1 compatibility
            except gdb.error:
                data = self.ref["m_holder"]["storage"]["dummy"].cast(
                    value_type.pointer()
                )  # redpanda 3.0 compatibility
        for i in range(self.__len__()):
            yield data[i]

    def __nonzero__(self):
        return self.__len__() > 0

    def __bool__(self):
        return self.__nonzero__()


class histogram:
    """Simple histogram.

    Aggregate items by their count and present them in a histogram format.
    Example:

        h = histogram()
        h['item1'] = 20 # Set an absolute value
        h.add('item2') # Equivalent to h['item2'] += 1
        h.add('item2')
        h.add('item3')
        h.print_to_console()

    Would print:
        4 item1 ++++++++++++++++++++++++++++++++++++++++
        2 item2 ++++
        1 item1 ++

    Note that the number of indicators ('+') is does not correspond to the
    actual number of items, rather it is supposed to illustrate their relative
    counts.
    """

    _column_count = 40

    def __init__(self, counts=None, print_indicators=True, formatter=None):
        """Constructor.

        Params:
        * counts: initial counts (default to empty).
        * print_indicators: print the '+' characters to illustrate relative
            count. Can be turned off when the item names are very long and would
            thus make indicators unreadable.
        * formatter: a callable that receives the item as its argument and is
            expected to return the string to be printed in the second column.
            By default, items are printed verbatim.
        """
        if counts is None:
            self._counts = defaultdict(int)
        else:
            self._counts = counts
        self._print_indicators = print_indicators

        def default_formatter(value):
            return str(value)

        if formatter is None:
            self._formatter = default_formatter
        else:
            self._formatter = formatter

    def __len__(self):
        return len(self._counts)

    def __nonzero__(self):
        return bool(len(self))

    def __getitem__(self, item):
        return self._counts[item]

    def __setitem__(self, item, value):
        self._counts[item] = value

    def add(self, item):
        self._counts[item] += 1

    def __str__(self):
        if not self._counts:
            return ""

        by_counts = defaultdict(list)
        for k, v in self._counts.items():
            by_counts[v].append(k)

        counts_sorted = list(reversed(sorted(by_counts.keys())))
        max_count = counts_sorted[0]

        if max_count == 0:
            count_per_column = 0
        else:
            count_per_column = self._column_count / max_count

        lines = []

        for count in counts_sorted:
            items = by_counts[count]
            if self._print_indicators:
                indicator = "+" * max(1, int(count * count_per_column))
            else:
                indicator = ""
            for item in items:
                lines.append(
                    "{:9d} {} {}".format(count, self._formatter(item), indicator)
                )

        return "\n".join(lines)

    def __repr__(self):
        return "histogram({})".format(self._counts)

    def print_to_console(self):
        gdb.write(str(self) + "\n")


def cpus():
    return int(gdb.parse_and_eval("::seastar::smp::count"))


def current_shard():
    return int(gdb.parse_and_eval("'seastar'::local_engine->_id"))


def get_local_task_queues():
    """Return a list of task pointers for the local reactor."""
    for tq_ptr in seastar_static_vector(
        gdb.parse_and_eval("'seastar'::local_engine._task_queues")
    ):
        yield std_unique_ptr(tq_ptr).dereference()


def get_local_tasks(tq_id=None):
    """Return a list of task pointers for the local reactor."""
    if tq_id is not None:
        tqs = filter(lambda x: x["_id"] == tq_id, get_local_task_queues())
    else:
        tqs = get_local_task_queues()

    for tq in tqs:
        for t in seastar_circular_buffer(tq["_q"]):
            yield t


# addr (int) -> name (str)
names = {}


def resolve(addr, cache=True, startswith=None):
    if addr in names:
        return names[addr]

    infosym = gdb.execute("info symbol 0x%x" % (addr), False, True)
    if infosym.startswith("No symbol"):
        return None

    name = infosym[: infosym.find("in section")]
    if startswith and not name.startswith(startswith):
        return None
    if cache:
        names[addr] = name
    return name


def get_reactor_backend():
    reactor_backend = gdb.parse_and_eval("seastar::local_engine->_backend")
    return std_unique_ptr(reactor_backend).get()


def get_text_range():
    try:
        vptr_type = gdb.lookup_type("uintptr_t").pointer()
        reactor_backend = gdb.parse_and_eval("seastar::local_engine->_backend")
        reactor_backend = std_unique_ptr(reactor_backend).get()
        # NOAH in clang it looks like things start with std::__1::unique_ptr
        ## 2019.1 has value member, >=3.0 has std::unique_ptr<>
        # if reactor_backend.type.strip_typedefs().name.startswith('std::unique_ptr<'):
        #    reactor_backend = std_unique_ptr(reactor_backend).get()
        # else:
        #    reactor_backend = gdb.parse_and_eval('&seastar::local_engine->_backend')
        known_vptr = int(reactor_backend.reinterpret_cast(vptr_type).dereference())
    except Exception as e:
        gdb.write(
            "get_text_range(): Falling back to locating .rodata section because lookup to reactor backend to use as known vptr failed: {}\n".format(
                e
            )
        )
        known_vptr = None

    sections = gdb.execute("info files", False, True).split("\n")
    for line in sections:
        if known_vptr:
            if not " is ." in line:
                continue
            items = line.split()
            start = int(items[0], 16)
            end = int(items[2], 16)
            if start <= known_vptr and known_vptr <= end:
                return start, end
        # vptrs are in .rodata section
        elif line.endswith("is .rodata"):
            items = line.split()
            text_start = int(items[0], 16)
            text_end = int(items[2], 16)
            return text_start, text_end

    raise Exception("Failed to find text start and end")


def find_vptrs():
    cpu_mem = gdb.parse_and_eval("'seastar::memory::cpu_mem'")
    page_size = int(gdb.parse_and_eval("'seastar::memory::page_size'"))
    mem_start = cpu_mem["memory"]
    vptr_type = gdb.lookup_type("uintptr_t").pointer()
    pages = cpu_mem["pages"]
    nr_pages = int(cpu_mem["nr_pages"])

    text_start, text_end = get_text_range()

    def is_vptr(addr):
        return addr >= text_start and addr <= text_end

    idx = 0
    while idx < nr_pages:
        if pages[idx]["free"]:
            idx += pages[idx]["span_size"]
            continue
        pool = pages[idx]["pool"]
        if not pool or pages[idx]["offset_in_span"] != 0:
            idx += 1
            continue
        objsize = int(pool.dereference()["_object_size"])
        span_size = pages[idx]["span_size"] * page_size
        for idx2 in range(0, int(span_size / objsize) + 1):
            obj_addr = mem_start + idx * page_size + idx2 * objsize
            vptr = obj_addr.reinterpret_cast(vptr_type).dereference()
            if is_vptr(vptr):
                yield obj_addr, vptr
        idx += pages[idx]["span_size"]


class span(object):
    """
    Represents seastar allocator's memory span
    """

    def __init__(self, index, start, page):
        """
        :param index: index into cpu_mem.pages of the first page of the span
        :param start: memory address of the first page of the span
        :param page: seastar::memory::page* for the first page of the span
        """
        self.index = index
        self.start = start
        self.page = page

    def is_free(self):
        return self.page["free"]

    def pool(self):
        """
        Returns seastar::memory::small_pool* of this span.
        Valid only when is_small().
        """
        return self.page["pool"]

    def is_small(self):
        return not self.is_free() and self.page["pool"]

    def is_large(self):
        return not self.is_free() and not self.page["pool"]

    def size(self):
        return int(self.page["span_size"])

    def used_span_size(self):
        """
        Returns the number of pages at the front of the span which are used by the allocator.

        Due to https://github.com/redpandadb/seastar/issues/625 there may be some
        pages at the end of the span which are not used by the small pool.
        We try to detect this. It's not 100% accurate but should work in most cases.

        Returns 0 for free spans.
        """
        n_pages = 0
        pool = self.page["pool"]
        if self.page["free"]:
            return 0
        if not pool:
            return self.page["span_size"]
        for idx in range(int(self.page["span_size"])):
            page = self.page.address + idx
            if (
                not page["pool"]
                or page["pool"] != pool
                or page["offset_in_span"] != idx
            ):
                break
            n_pages += 1
        return n_pages


def spans():
    cpu_mem = gdb.parse_and_eval("'seastar::memory::cpu_mem'")
    page_size = int(gdb.parse_and_eval("'seastar::memory::page_size'"))
    nr_pages = int(cpu_mem["nr_pages"])
    pages = cpu_mem["pages"]
    mem_start = int(cpu_mem["memory"])
    idx = 1
    while idx < nr_pages:
        page = pages[idx]
        span_size = int(page["span_size"])
        if span_size == 0:
            idx += 1
            continue
        last_page = pages[idx + span_size - 1]
        addr = mem_start + idx * page_size
        yield span(idx, addr, page)
        idx += span_size


class span_checker(object):
    def __init__(self):
        self._page_size = int(gdb.parse_and_eval("'seastar::memory::page_size'"))
        span_list = list(spans())
        self._start_to_span = dict((s.start, s) for s in span_list)
        self._starts = list(s.start for s in span_list)

    def spans(self):
        return self._start_to_span.values()

    def get_span(self, ptr):
        idx = bisect.bisect_right(self._starts, ptr)
        if idx == 0:
            return None
        span_start = self._starts[idx - 1]
        s = self._start_to_span[span_start]
        if span_start + s.page["span_size"] * self._page_size <= ptr:
            return None
        return s


def find_storage_api(shard=None):
    if shard is None:
        shard = current_shard()
    return gdb.parse_and_eval("debug::app")["storage"]["_instances"]["__begin_"][shard][
        "service"
    ]["_p"]


def find_partition_manager(shard=None):
    if shard is None:
        shard = current_shard()
    return gdb.parse_and_eval("debug::app")["partition_manager"]["_instances"][
        "__begin_"
    ][shard]["service"]["_p"]


def find_cloud_storage_clients(shard=None):
    if shard is None:
        shard = current_shard()
    return gdb.parse_and_eval("debug::app")["cloud_storage_clients"]["_instances"][
        "__begin_"
    ][shard]["service"]["_p"]


class index_state:
    def __init__(self, ref):
        self.ref = ref
        self.offset = chunked_vector(self.ref["relative_offset_index"])
        self.time = chunked_vector(self.ref["relative_time_index"])
        self.pos = chunked_vector(self.ref["position_index"])

    def size(self):
        return int(
            self.offset.size_bytes() + self.time.size_bytes() + self.pos.size_bytes()
        )

    def capacities(self):
        return (
            int(x)
            for x in (
                self.offset.size_bytes_capacity(),
                self.time.size_bytes_capacity(),
                self.pos.size_bytes_capacity(),
            )
        )

    def capacity(self):
        return int(sum(self.capacities()))

    def __str__(self):
        s = self.size() // 1024
        c = self.capacity() // 1024
        p = [x // 1024 for x in self.capacities()]
        return f"Size (KB) {s:4} Capacity {c:4} (Contig off={p[0]:4} time={p[1]:4} pos={p[2]:4})"


class segment_index:
    def __init__(self, ref):
        self.ref = ref

    def name(self):
        return self.ref["_name"]

    def state(self):
        return index_state(self.ref["_state"])


class segment_reader:
    def __init__(self, ref):
        self.ref = ref
        self.path = ref["_path"]
        self.streams = boost_intrusive_list(self.ref["_streams"], "_hook")
        for s in self.streams:
            opt_iss = std_optional(s["_stream"])
            print(f"Stream: has_value: {bool(opt_iss)}")

    def __str__(self):
        return "{}".format(self.path)


class spill_key_index:
    spill_key_index_t = gdb.lookup_type("storage::internal::spill_key_index")

    def __init__(self, name, ref):
        self._name = name
        self.ref = ref.cast(self.spill_key_index_t.pointer())

    def name(self):
        return self._name

    def index(self):
        return absl_flat_hash_map(self.ref["_midx"])


class model_offset:
    def __init__(self, ref):
        self.ref = ref

    def __str__(self):
        return str(self.ref["_value"])


class offset_tracker:
    def __init__(self, ref):
        self.ref = ref

    @property
    def base_offset(self):
        return model_offset(self.ref["_base_offset"])

    @property
    def dirty_offset(self):
        return model_offset(self.ref["_dirty_offset"])

    @property
    def term(self):
        return model_offset(self.ref["_term"])

    @property
    def committed_offset(self):
        return model_offset(self.ref["_committed_offset"])

    @property
    def stable_offset(self):
        return model_offset(self.ref["_stable_offset"])

    def __str__(self):
        return f"[base_offset: {self.base_offset}, dirty_offset: {self.dirty_offset}, term: {self.term} committed_offset: {self.committed_offset}, stable_offset: {self.stable_offset}]"


class segment:
    segment_t = gdb.lookup_type("storage::segment")
    segment_t_size = segment_t.sizeof

    def __init__(self, ref):
        self.ref = ref

    def compacted_index_writer(self):
        o = std_optional(self.ref["_compaction_index"])
        if o:
            impl = std_unique_ptr(o.get()["_impl"]).get()
            name = impl["_name"]
            return spill_key_index(name, impl)

    def batch_cache_index(self):
        o = std_optional(self.ref["_cache"])
        if o:
            return absl_btree_map(o.get()["_index"])

    def offsets_tracker(self):
        return offset_tracker(self.ref["_tracker"])

    def destructive_ops(self):
        return seastar_basic_rwlock(self.ref["_destructive_ops"])

    def reader(self):
        return segment_reader(std_unique_ptr(self.ref["_reader"]).get().dereference())

    def index(self):
        return segment_index(self.ref["_idx"])

    def batch_cache_index_size_bytes(self):
        index_opt = std_optional(self.ref["_cache"])
        if not index_opt:
            return 0
        tree = btree_map(index_opt.get()["_index"])
        return (24 + 8) * tree.size(), tree.size()


class segment_set:
    def __init__(self, ref):
        self.ref = ref

    def size(self):
        return seastar_circular_buffer(self.ref["_handles"]).size()

    def __iter__(self):
        segments = seastar_circular_buffer(self.ref["_handles"])
        for ptr in segments:
            yield segment(seastar_lw_shared_ptr(ptr).get())


class model_ntp:
    def __init__(self, ref):
        self.ref = ref

    def namespace(self):
        return self.ref["ns"]["_value"]

    def topic(self):
        return self.ref["tp"]["topic"]["_value"]

    def partition(self):
        return self.ref["tp"]["partition"]["_value"]

    def __repr__(self):
        return f"{self.namespace()}/{self.topic()}/{self.partition()}"


def template_arguments(gdb_type):
    n = 0
    while True:
        try:
            yield gdb_type.template_argument(n)
            n += 1
        except RuntimeError:
            return


def get_template_arg_with_prefix(gdb_type, prefix):
    for arg in template_arguments(gdb_type):
        if str(arg).startswith(prefix):
            return arg


def get_field_offset(gdb_type, name):
    for field in gdb_type.fields():
        if field.name == name:
            return int(field.bitpos / 8)


def get_base_class_offset(gdb_type, base_class_name):
    name_pattern = re.escape(base_class_name) + "(<.*>)?$"
    for field in gdb_type.fields():
        if field.is_base_class and re.match(
            name_pattern, field.type.strip_typedefs().name
        ):
            return int(field.bitpos / 8)


class boost_intrusive_list:
    size_t = gdb.lookup_type("size_t")

    def __init__(self, list_ref, link=None):
        list_type = list_ref.type.strip_typedefs()
        self.node_type = list_type.template_argument(0)
        rps = list_ref["data_"]["root_plus_size_"]
        try:
            self.root = rps["root_"]
        except gdb.error:
            # Some boost versions have this instead
            self.root = rps["m_header"]
        if link is not None:
            self.link_offset = get_field_offset(self.node_type, link)
        else:
            member_hook = get_template_arg_with_prefix(
                list_type, "boost::intrusive::member_hook"
            )

            if not member_hook:
                member_hook = get_template_arg_with_prefix(
                    list_type, "struct boost::intrusive::member_hook"
                )
            if member_hook:
                self.link_offset = member_hook.template_argument(2).cast(self.size_t)
            else:
                self.link_offset = get_base_class_offset(
                    self.node_type, "boost::intrusive::list_base_hook"
                )
                if self.link_offset is None:
                    raise Exception(
                        "Class does not extend list_base_hook: " + str(self.node_type)
                    )

    def __iter__(self):
        hook = self.root["next_"]
        while hook and hook != self.root.address:
            node_ptr = hook.cast(self.size_t) - self.link_offset
            yield node_ptr.cast(self.node_type.pointer()).dereference()
            hook = hook["next_"]

    def __nonzero__(self):
        return self.root["next_"] != self.root.address

    def __bool__(self):
        return self.__nonzero__()

    def __len__(self):
        return len(list(iter(self)))

    def __repr__(self):
        return ",".join([str(x) for x in self])


class readers_cache:
    def __init__(self, ref):
        self.ref = ref
        self.readers = boost_intrusive_list(self.ref["_readers"], "_hook")
        self.in_use = boost_intrusive_list(self.ref["_in_use"], "_hook")


class disk_log_impl:
    disk_log_impl_t = gdb.lookup_type("storage::disk_log_impl")

    def __init__(self, ref):
        self.ref = ref.cast(self.disk_log_impl_t.pointer())

    def segments(self):
        return segment_set(self.ref["_segs"])

    def readers_cache(self):
        return readers_cache(std_unique_ptr(self.ref["_readers_cache"]).get())


class log_housekeeping_meta:
    """
    This is based on the v24.2.x implementation. When implementing decoding for
    newer versions it is often sufficient to use a try/catch block to
    incrementally fall back when field names and types change.
    """

    log_housekeeping_meta_t = gdb.lookup_type("storage::log_housekeeping_meta")

    def __init__(self, ref):
        self.ref = ref.cast(self.log_housekeeping_meta_t.pointer())

    def handle(self):
        h = self.ref["handle"]
        return seastar_shared_ptr(h).get()


def find_logs(shard=None):
    storage = find_storage_api(shard)
    log_mgr = std_unique_ptr(storage["_log_mgr"]).dereference()
    for ntp, log in absl_flat_hash_map(log_mgr["_logs"]):
        meta = log_housekeeping_meta(std_unique_ptr(log).get())
        impl = meta.handle()
        yield ntp, disk_log_impl(impl)


class redpanda_memory(gdb.Command):
    """Summarize the state of the shard's memory.

    The goal of this summary is to provide a starting point when investigating
    memory issues.

    The summary consists of two parts:
    * A high level overview.
    * A per size-class population statistics.

    In an OOM situation the latter usually shows the immediate symptoms, one
    or more heavily populated size classes eating up all memory. The overview
    can be used to identify the subsystem that owns these problematic objects.
    """

    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda memory", gdb.COMMAND_USER, gdb.COMPLETE_COMMAND
        )

    def print_kvstore_memory(self):
        storage = find_storage_api()
        kvstore = std_unique_ptr(storage["_kvstore"]).dereference()
        db = absl_flat_hash_map(kvstore["_db"])
        print(f"# Key value store")
        gdb.write("key-value store:\n")
        gdb.write("      size: {}\n".format(len(db)))
        gdb.write("  capacity: {}\n".format(db.capacity()))
        gdb.write("size bytes: {}\n".format(kvstore["_probe"]["cached_bytes"]))

    def print_segment_memory(self):
        print(f"# Log segments")
        sizes = []
        capacities = []
        contigs = []
        for ntp, log in find_logs():
            for segment in log.segments():
                index = segment.index().state()
                contigs += index.capacities()
                size, capacity = index.size(), index.capacity()
                sizes.append(size)
                capacities.append(capacity)
                print(f"Partition {index} @ {ntp}")

        print(f"Number of segments: {len(sizes)}")
        print(f"Total capacity: {sum(capacities) // 1024} KB")
        print("Contiguous allocations (KB)")
        contig_kb_counts = Counter((x // 1024 for x in contigs))
        for size, freq in contig_kb_counts.most_common():
            print(f"Size {size:4} Freq {freq}")

    def print_readers_cache_memory(self):
        print(f"# Readers cache")
        total_readers = 0
        for ntp, log in find_logs():
            readers = len(log.readers_cache().readers)
            in_use = len(log.readers_cache().in_use)
            print(f"readers: {readers}, readers_in_use: {in_use} @ {ntp}")
            total_readers += in_use
            total_readers += readers

        print(f"Total cached readers: {total_readers}")

    def invoke(self, arg, from_tty):
        self.print_kvstore_memory()
        self.print_segment_memory()
        self.print_readers_cache_memory()

        cpu_mem = gdb.parse_and_eval("'seastar::memory::cpu_mem'")
        page_size = int(gdb.parse_and_eval("'seastar::memory::page_size'"))
        free_mem = int(cpu_mem["nr_free_pages"]) * page_size
        total_mem = int(cpu_mem["nr_pages"]) * page_size
        gdb.write(
            "Used memory: {used_mem:>13}\nFree memory: {free_mem:>13}\nTotal memory: {total_mem:>12}\n\n".format(
                used_mem=total_mem - free_mem, free_mem=free_mem, total_mem=total_mem
            )
        )

        gdb.write("Small pools:\n")
        small_pools = cpu_mem["small_pools"]
        nr = small_pools["nr_small_pools"]
        gdb.write(
            "{objsize:>5} {span_size:>6} {use_count:>10} {memory:>12} {unused:>12} {wasted_percent:>5}\n".format(
                objsize="objsz",
                span_size="spansz",
                use_count="usedobj",
                memory="memory",
                unused="unused",
                wasted_percent="wst%",
            )
        )
        total_small_bytes = 0
        sc = span_checker()
        for i in range(int(nr)):
            sp = small_pools["_u"]["a"][i]
            object_size = int(sp["_object_size"])
            span_size = int(sp["_span_sizes"]["preferred"]) * page_size
            free_count = int(sp["_free_count"])
            pages_in_use = 0
            use_count = 0
            for s in sc.spans():
                if s.pool() == sp.address:
                    pages_in_use += s.size()
                    use_count += int(s.used_span_size() * page_size / object_size)
            memory = pages_in_use * page_size
            total_small_bytes += memory
            use_count -= free_count
            wasted = free_count * object_size
            unused = memory - use_count * object_size
            wasted_percent = wasted * 100.0 / memory if memory else 0
            gdb.write(
                "{objsize:5} {span_size:6} {use_count:10} {memory:12} {unused:12} {wasted_percent:5.1f}\n".format(
                    objsize=object_size,
                    span_size=span_size,
                    use_count=use_count,
                    memory=memory,
                    unused=unused,
                    wasted_percent=wasted_percent,
                )
            )
        gdb.write("Small allocations: %d [B]\n" % total_small_bytes)

        large_allocs = defaultdict(int)  # key: span size [B], value: span count
        for s in sc.spans():
            span_size = s.size()
            if s.is_large():
                large_allocs[span_size * page_size] += 1

        gdb.write("Page spans:\n")
        gdb.write(
            "{index:5} {size:>13} {total:>13} {allocated_size:>13} {allocated_count:>7}\n".format(
                index="index",
                size="size [B]",
                total="free [B]",
                allocated_size="large [B]",
                allocated_count="[spans]",
            )
        )
        total_large_bytes = 0
        for index in range(int(cpu_mem["nr_span_lists"])):
            span_list = cpu_mem["free_spans"][index]
            front = int(span_list["_front"])
            pages = cpu_mem["pages"]
            total = 0
            while front:
                span = pages[front]
                total += int(span["span_size"])
                front = int(span["link"]["_next"])
            span_size = (1 << index) * page_size
            allocated_size = large_allocs[span_size] * span_size
            total_large_bytes += allocated_size
            gdb.write(
                "{index:5} {size:13} {total:13} {allocated_size:13} {allocated_count:7}\n".format(
                    index=index,
                    size=span_size,
                    total=total * page_size,
                    allocated_count=large_allocs[span_size],
                    allocated_size=allocated_size,
                )
            )
        gdb.write("Large allocations: %d [B]\n" % total_large_bytes)


class redpanda_storage(gdb.Command):
    """Summarize the state of redpanda storage layer"""

    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda storage", gdb.COMMAND_USER, gdb.COMPLETE_COMMAND
        )

    def print_segments(self):
        print(f"# Log segments")

        for ntp, log in find_logs():
            print(f"{ntp} segment count {log.segments().size()}")
            for segment in log.segments():
                offsets = segment.offsets_tracker()
                # destructive_ops = segment.destructive_ops()
                print(f"{ntp} - {offsets} - {0} - reader: {segment.reader()}")
                break

    def print_readers_cache_memory(self):
        print(f"# Readers cache")
        total_readers = 0
        for ntp, log in find_logs():
            readers = len(log.readers_cache().readers)
            in_use = len(log.readers_cache().in_use)
            print(f"readers: {readers}, readers_in_use: {in_use} @ {ntp}")
            total_readers += in_use
            total_readers += readers

        print(f"Total cached readers: {total_readers}")

    def invoke(self, arg, from_tty):
        self.print_segments()
        self.print_readers_cache_memory()


class chunked_fifo:
    class chunk:
        def __init__(self, ref):
            self.items = ref["items"]
            self.begin = ref["begin"]
            self.end = ref["end"]
            self.next_one = ref["next"]

        def __len__(self):
            return self.end - self.begin

    class iterator:
        def __init__(self, fifo, chunk):
            self.fifo = fifo
            self.chunk = chunk
            self.index = self.chunk.begin if chunk else 0

        def __next__(self):
            if self.index == 0:
                raise StopIteration
            if self.index == self.chunk.end:
                if not self.chunk.next_one:
                    raise StopIteration
                self.chunk = chunked_fifo.chunk(self.chunk.next_one)
                self.index = self.chunk.begin
            index = self.index
            self.index += 1
            return self.chunk.items[self.fifo.mask(index)]["data"]

    def __init__(self, ref):
        self._ref = ref
        # try to access the member variable, so the constructor throws if the
        # inspected variable is of the wrong type
        _ = self.front_chunk

    def mask(self, index):
        return index & (self.items_per_chunk - 1)

    @property
    def items_per_chunk(self):
        return self._ref.type.template_argument(1)

    @property
    def front_chunk(self):
        return self._ref["_front_chunk"]

    @property
    def back_chunk(self):
        return self._ref["_back_chunk"]

    def __len__(self):
        if not self.front_chunk:
            return 0
        if self.back_chunk == self.front_chunk:
            front_chunk = self.chunk(self.front_chunk.dereference())
            return len(front_chunk)
        else:
            front_chunk = self.chunk(self.front_chunk.dereference())
            back_chunk = self.chunk(self.back_chunk.dereference())
            num_chunks = self._ref["_nchunks"] - 2
            return (
                len(front_chunk) + len(back_chunk) + num_chunks * self.items_per_chunk
            )

    def __iter__(self):
        return self.iterator(self, self.front_chunk)


class abortable_fifo:
    class entry:
        def __init__(self, ref):
            self.ref = ref
            self.payload = ref["payload"]

    def __init__(self, ref):
        self.ref = ref
        self.front = abortable_fifo.entry(std_unique_ptr(ref["_front"]).dereference())
        self.list = chunked_fifo(ref["_list"])
        self.size = self.ref["_size"]

    class iterator:
        def __init__(self, fifo):
            self.fifo = fifo
            self.index = 0
            self.it = iter(self.fifo.list)

        def __next__(self):
            if self.fifo.size == 0:
                raise StopIteration
            if self.index == 0:
                self.index += 1
                return self.fifo.front
            else:
                return abortable_fifo.entry(next(self.it))

    def __iter__(self):
        return self.iterator(self)


class named_samaphore:
    def __init__(self, ref):
        self.ref = ref
        self.wait_list = abortable_fifo(ref["_wait_list"])
        self.count = ref["_count"]

    def __repr__(self):
        return (
            f"named_samaphore(count={self.count}, wait_list_size={self.wait_list.size})"
        )


class offset_monitor:
    def __init__(self, ref):
        self.ref = ref
        self.waiters_size = self.ref["_waiters"]["tree_"]["size_"]

    def __repr__(self):
        return f"offset_monitor(waiters_size={self.waiters_size})"


class condition_variable:
    def __init__(self, ref):
        self.ref = ref
        self.waiters = boost_intrusive_list(ref["_waiters"])
        self.signaled = ref["_signalled"]
        self.ex = ref["_ex"]

    def __repr__(self):
        return f"condition_variable(waiters_count={len(self.waiters)}, signalled={self.signaled}, ex={self.ex})"


def lowres_clock_now():
    """Returns the current time in lowres_clock format."""
    return gdb.parse_and_eval("seastar::lowres_clock::_now")


class time_point:
    NOW = lowres_clock_now()["__d_"]["__rep_"]

    def __init__(self, ref) -> None:
        self.value = ref["__d_"]["__rep_"]
        self.time_from_now = time_point.NOW - self.value

    def __repr__(self):
        return f"time_point(value={self.value}, time_from_now={self.time_from_now / 1000000}ms)"


class abort_source:
    def __init__(self, ref):
        self.ref = ref
        self.ex = ref["_ex"]
        self.subscriptions = boost_intrusive_list(ref["_subscriptions"])

    def __repr__(self):
        return f"abort_source(abort_requested={self.ex}, subscriptions_count={len(self.subscriptions)})"


class seastar_output_stream:
    def __init__(self, ref):
        self.ref = ref
        self.fd = ref["_fd"]
        self._buf = ref["_buf"]
        self.size = ref["_size"]
        self.begin = ref["_begin"]
        self.end = ref["_end"]
        self.trim_to_size = ref["_trim_to_size"]
        self.batch_flushes = ref["_batch_flushes"]
        self.in_batch = std_optional(ref["_in_batch"])
        if self.in_batch:
            self.in_batch = seastar_promise(self.in_batch.get())
        self.flush = ref["_flush"]
        self.flushing = ref["_flushing"]
        self.ex = ref["_ex"]

    def __repr__(self):
        return f"seastar_output_stream(fd={self.fd}, size={self.size}, begin={self.begin}, end={self.end}, trim_to_size={self.trim_to_size}, batch_flushes={self.batch_flushes}, in_batch={self.in_batch}, flush={self.flush}, flushing={self.flushing}, ex={self.ex})"


class seastar_pollable_fd:
    def __init__(self, ref):
        self.ref = ref
        self.state_ptr = ref["_s"]["px"]
        self.state = self.state_ptr.dynamic_cast(
            gdb.lookup_type("seastar::aio_pollable_fd_state").pointer()
        ).dereference()
        # self.state_completion_pollin = seastar_fd_state_completion(self.state['_completion_pollin'])
        # self.state_completion_pollout = seastar_fd_state_completion(self.state['_completion_pollout'])
        # self.state_completion_pollrdhup = seastar_fd_state_completion(self.state['_completion_pollrdhup'])

    def __repr__(self):
        return f"pollable_fd(state={self.state})"
        # return f"pollable_fd(state={self.state}, pollin={self.state_completion_pollin}, pollout={self.state_completion_pollout}, pollrdhup={self.state_completion_pollrdhup})"


class seastar_fd_state_completion:
    def __init__(self, ref):
        self.ref = ref
        self.pr = seastar_promise(ref["_pr"])

    def __repr__(self):
        return f"pollable_fd_state_completion(pr={self.pr})"


class statem:
    def __init__(self, ref):
        self.ref = ref
        self.state = ref["state"]
        self.write_state = ref["write_state"]
        self.write_state_work = ref["write_state_work"]
        self.read_state = ref["read_state"]
        self.read_state_work = ref["read_state_work"]
        self.hand_state = ref["hand_state"]
        self.request_state = ref["request_state"]
        self.in_init = ref["in_init"]
        self.in_handshake = ref["in_handshake"]
        self.cleanuphand = ref["cleanuphand"]

    def __repr__(self):
        return f"statem(state={self.state}, write_state={self.write_state}, write_state_work={self.write_state_work}, read_state={self.read_state}, read_state_work={self.read_state_work}, hand_state={self.hand_state}, request_state={self.request_state}, in_init={self.in_init}, in_handshake={self.in_handshake}, cleanuphand={self.cleanuphand})"


class rlayer:
    def __init__(self, ref):
        self.ref = ref
        self.read_ahead = ref["read_ahead"]
        self.rstate = ref["rstate"]
        self.packet_length = ref["packet_length"]
        self.wnum = ref["wnum"]
        self.empty_record_count = ref["empty_record_count"]
        self.wpend_tot = ref["wpend_tot"]
        self.wpend_type = ref["wpend_type"]
        self.wpend_ret = ref["wpend_ret"]

    def __repr__(self):
        return f"rlayer(read_ahead={self.read_ahead}, rstate={self.rstate}, packet_length={self.packet_length}, wnum={self.wnum}, empty_record_count={self.empty_record_count}, wpend_tot={self.wpend_tot}, wpend_type={self.wpend_type}, wpend_ret={self.wpend_ret})"


class ssl_st:
    def __init__(self, ref):
        self.ref = ref
        self.version = ref["version"]
        self.error = ref["error"]
        self.error_code = ref["error_code"]
        self.init_num = ref["init_num"]
        self.init_off = ref["init_off"]
        self.renegotiate = ref["renegotiate"]
        self.key_update = ref["key_update"]
        self.s3_renegotiation = ref["s3"]["renegotiate"]
        self.s3_total_renegotiations = ref["s3"]["total_renegotiations"]
        self.s3_num_renegotiations = ref["s3"]["num_renegotiations"]
        self.s3_change_cipher_spec = ref["s3"]["change_cipher_spec"]
        self.s3_warn_alert = ref["s3"]["warn_alert"]
        self.s3_fatal_alert = ref["s3"]["fatal_alert"]
        self.rlayer = rlayer(ref["rlayer"])
        self.statem = statem(ref["statem"])

    def __repr__(self):
        return f"ssl_st(statem={self.statem}, rlayer={self.rlayer}, version={self.version}, error={self.error}, error_code={self.error_code}, init_num={self.init_num}, init_off={self.init_off}, renegotiate= {self.renegotiate}, key_update= {self.key_update}, s3_renegotiation= {self.s3_renegotiation}, s3_total_renegotiations= {self.s3_total_renegotiations}, s3_num_renegotiations= {self.s3_num_renegotiations}, s3_change_cipher_spec= {self.s3_change_cipher_spec}, s3_warn_alert= {self.s3_warn_alert}, s3_fatal_alert= {self.s3_fatal_alert}"


class seastar_data_source:
    def __init__(self, ref):
        self.ref = ref
        self.casted = ref.address.dynamic_cast(
            gdb.lookup_type(
                "seastar::tls::tls_connected_socket_impl::source_impl"
            ).pointer()
        ).dereference()
        self.session = (
            seastar_shared_ptr(self.casted["_session"])
            .get()
            .dynamic_cast(gdb.lookup_type("seastar::tls::session").pointer())
            .dereference()
        )
        self.session_in_sem = named_samaphore(self.session["_in_sem"])
        self.session_out_sem = named_samaphore(self.session["_out_sem"])
        self.session_input = self.session["_input"]
        self.session_eof = self.session["_eof"]

        self.session_error = self.session["_error"]
        self.session_shutdown = self.session["_shutdown"]
        self.session_out_pending = self.session["_output_pending"]
        self.session_in = (
            std_unique_ptr(self.session["_in"]["_dsi"])
            .get()
            .dynamic_cast(
                gdb.lookup_type("seastar::net::posix_data_source_impl").pointer()
            )
            .dereference()
        )
        self.out_pending = self.session["_output_pending"]
        self.session_ssl = std_unique_ptr(self.session["_ssl"]).get().dereference()
        self.rbio = self.session_ssl["rbio"].dereference()
        self.wbio = self.session_ssl["wbio"].dereference()

        self.session_sock = (
            std_unique_ptr(self.session["_sock"])
            .get()
            .dynamic_cast(
                gdb.lookup_type("seastar::net::posix_connected_socket_impl").pointer()
            )
            .dereference()
        )
        self.session_sock_fd = seastar_pollable_fd(self.session_sock["_fd"])

    def __repr__(self):
        return f"""
seastar_data_source(ref={self.ref},
session_in_sem={self.session_in_sem},
session_out_sem={self.session_out_sem},
session_input={self.session_input},
session_eof={self.session_eof},
session_error={self.session_error},
session_shutdown={self.session_shutdown},
session_out_pending={self.session_out_pending},
session_sock_fd={self.session_sock_fd},
session_sock={self.session_sock},
ssl = {self.session_ssl},
rbio={self.rbio},
wbio={self.wbio},
session_in={self.session_in},
out_pending={self.out_pending}),
"""


class seastar_input_stream:
    def __init__(self, ref):
        self.ref = ref
        self.fd_ptr = std_unique_ptr(ref["_fd"]["_dsi"])
        self.fd = None
        if self.fd_ptr:
            self.fd = seastar_data_source(self.fd_ptr.get().dereference())
        self.buf = ref["_buf"]
        self.eof = ref["_eof"]

    def __repr__(self):
        return f"seastar_input_stream(fd={self.fd}, eof={self.eof}, buf={self.buf})"


class batched_output_stream:
    def __init__(self, ref):
        self.ref = ref
        self.out = seastar_output_stream(ref["_out"])
        self.cache_size = ref["_cache_size"]
        self.unflushed_bytes = ref["_unflushed_bytes"]
        self.write_sem_ptr = std_unique_ptr(ref["_write_sem"])

        if self.write_sem_ptr:
            self.write_sem = named_samaphore(self.write_sem_ptr.get().dereference())
        else:
            self.write_sem = None
        self._closed = ref["_closed"]

    def __repr__(self):
        return f"batched_output_stream(out={self.out},cache_size={self.cache_size}, unflushed_bytes={self.unflushed_bytes}, write_sem={self.write_sem}, closed={self._closed})"


class net_base_transport:
    def __init__(self, ref):
        self.ref = ref
        self.out = batched_output_stream(ref["_out"])
        self.ins = seastar_input_stream(ref["_in"])

    def __repr__(self):
        return f"net_base_transport(out={self.out},in={self.ins})"


class cloud_client_ptr:
    def __init__(self, shared_ptr):
        self.client_raw_ptr = seastar_shared_ptr(shared_ptr).get()
        self.client = self.client_raw_ptr.dereference()
        self.s3_client = self.client_raw_ptr.dynamic_cast(
            gdb.lookup_type("cloud_storage_clients::s3_client").pointer()
        ).dereference()
        self.http_client = self.s3_client["_client"]
        self.http_client_last = time_point(self.s3_client["_client"]["_last_response"])
        self.probe = seastar_shared_ptr(self.http_client["_probe"]).get().dereference()
        self.abort_source = abort_source(self.http_client["_as"].dereference())
        self.http_connect_gate_count = self.http_client["_connect_gate"]["_count"]
        self.base_transport = net_base_transport(
            self.http_client.address.dynamic_cast(
                gdb.lookup_type("net::base_transport").pointer()
            ).dereference()
        )

    def __repr__(self):
        return f"client(last_response={self.http_client_last}, probe={self.probe}, as={self.abort_source}, bt={self.base_transport}, connect_gate_count={self.http_connect_gate_count})"


class cloud_client_lease:
    def __init__(self, ref):
        self.ref = ref
        self.client = cloud_client_ptr(ref["client"])

    def __repr__(self):
        return f"lease(client={self.client})"


class cloud_client_pool:
    def __init__(self, ref):
        self.ref = ref
        self.pool = [cloud_client_ptr(c) for c in seastar_circular_buffer(ref["_pool"])]
        self.leased = [
            cloud_client_lease(l) for l in boost_intrusive_list(ref["_leased"], "_hook")
        ]

    def __repr__(self):
        return f"client_pool(pool={self.pool}, leased={self.leased})"


class cloud_storage_remote:
    def __init__(self, ref):
        self.ref = ref
        self.gate_count = ref["_gate"]["_count"]

    def __repr__(self):
        return f"cloud_storage_remote(gate_count={self.gate_count})"


class ntp_archiver:
    def __init__(self, ref, shard=None):
        self.ref = ref
        self.mutex = named_samaphore(ref["_mutex"])
        self.last_upload_time = time_point(ref["_last_upload_time"])
        self.uploads_active = named_samaphore(ref["_uploads_active"])
        self.start_term = model_offset(ref["_start_term"])
        self.last_marked_clean_time = time_point(ref["_last_marked_clean_time"])
        self.gate_count = ref["_gate"]["_count"]
        self.paused = ref["_paused"]
        self.leader_cond = condition_variable(ref["_leader_cond"])
        self.flush_cond = condition_variable(ref["_flush_cond"])
        self.remote = cloud_storage_remote(ref["_remote"].referenced_value())

    def __repr__(self):
        return f"ntp_archiver(mutex={self.mutex}, start_term={self.start_term}, last_upload_time={self.last_upload_time}, uploads_active={self.uploads_active}, last_marked_clean_time={self.last_marked_clean_time}, gate_count={self.gate_count}, paused={self.paused}, leader_cond={self.leader_cond}, flush_cond={self.flush_cond}, remote={self.remote})"


class archival_metadata_stm:
    def __init__(self, ref):
        self.ref = ref
        self.lock = named_samaphore(ref["_lock"]["_sem"])
        self.last_clean_at = model_offset(ref["_last_clean_at"])
        self.last_dirty_at = model_offset(ref["_last_dirty_at"])
        self.op_lock = named_samaphore(ref["_op_lock"]["_sem"])
        self.apply_lock = named_samaphore(ref["_apply_lock"]["_sem"])

    def __repr__(self):
        return f"archival_metadata_stm(lock={self.lock}, last_clean_at={self.last_clean_at}, last_dirty_at={self.last_dirty_at}, op_lock={self.op_lock}, apply_lock={self.apply_lock})"


class log_eviction_stm:
    def __init__(self, ref):
        self.ref = ref
        self.storage_eviction_offset = model_offset(ref["_storage_eviction_offset"])
        self.delete_records_eviction_offset = model_offset(
            ref["_delete_records_eviction_offset"]
        )
        self.cached_kafka_offset_override = model_offset(
            ref["_cached_kafka_start_offset_override"]
        )
        self.has_pending_truncation = condition_variable(ref["_has_pending_truncation"])
        self.gate_count = ref["_gate"]["_count"]
        self.op_lock = named_samaphore(ref["_op_lock"]["_sem"])
        self.apply_lock = named_samaphore(ref["_apply_lock"]["_sem"])

    def __repr__(self):
        return f"log_eviction_stm(has_pending_truncation={self.has_pending_truncation}, storage_eviction_offset={self.storage_eviction_offset}, delete_records_eviction_offset={self.delete_records_eviction_offset}, cached_kafka_offset_override={self.cached_kafka_offset_override}, gate_count={self.gate_count}, op_lock={self.op_lock}, apply_lock={self.apply_lock})"


class rm_stm:
    def __init__(self, ref):
        self.ref = ref
        self.state_lock = seastar_basic_rwlock(ref["_state_lock"])
        self.last_known_lso = model_offset(ref["_last_known_lso"])
        self.op_lock = named_samaphore(ref["_op_lock"]["_sem"])
        self.apply_lock = named_samaphore(ref["_apply_lock"]["_sem"])

    def __repr__(self):
        return f"rm_stm(state_lock={self.state_lock}, last_known_lso={self.last_known_lso}, op_lock={self.op_lock}, apply_lock={self.apply_lock})"


class consensus:
    def __init__(self, ref):
        self.ref = ref
        self.term = ref["_term"]
        self.confirmed_term = ref["_confirmed_term"]
        self.election_lock = named_samaphore(ref["_election_lock"]["_sem"])
        self.op_lock = named_samaphore(ref["_op_lock"]["_sem"])
        self.snapshot_lock = named_samaphore(ref["_snapshot_lock"]["_sem"])
        self.v_state = ref["_vstate"]
        self.offset_monitor = offset_monitor(ref["_consumable_offset_monitor"])

    # vote state reference: leader = 2, follower = 1, candidate = 0
    def is_leader(self):
        return self.v_state == 2 and self.term == self.confirmed_term

    def __repr__(self):
        return f"consensus(term={self.term}, confirmed_term={self.confirmed_term}, v_state={self.v_state}, is_leader={self.is_leader()}, election_lock={self.election_lock}, op_lock={self.op_lock}, snapshot_lock={self.snapshot_lock}, offset_monitor={self.offset_monitor})"


def parse_shard_arg(arg):
    if arg is None or arg == "":
        return None
    return int(arg)


class redpanda_partition:
    def __init__(self, ptr):
        self.ptr = ptr
        self.archiver = ntp_archiver(std_unique_ptr(ptr["_archiver"]).dereference())
        self.archival_meta = archival_metadata_stm(
            seastar_shared_ptr(ptr["_archival_meta_stm"]).get()
        )
        self.rm = rm_stm(seastar_shared_ptr(ptr["_rm_stm"]).get())
        self.log_eviction = log_eviction_stm(
            seastar_shared_ptr(ptr["_log_eviction_stm"]).get()
        )
        self.raft = consensus(seastar_lw_shared_ptr(ptr["_raft"]).get())


class redpanda_partitions(gdb.Command):
    """Summarize the state of redpanda storage layer"""

    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda partitions", gdb.COMMAND_USER, gdb.COMPLETE_COMMAND
        )

    def print_partitions(self, cpu=None):
        cpu_list = range(cpus()) if cpu is None else [int(cpu)]
        for i in cpu_list:
            print(f"# Partitions on shard {i}")
            pm_ptr = find_partition_manager(i)

            for v in absl_get_nodes(pm_ptr["_ntp_table"]):
                try:
                    ntp = v["value"]["first"]
                    p = redpanda_partition(
                        seastar_lw_shared_ptr(v["value"]["second"]).get()
                    )
                    print(
                        "ntp: {}\n {}\n, {}\n, {}\n, {}\n, {}\n".format(
                            model_ntp(ntp),
                            p.archiver,
                            p.archival_meta,
                            p.rm,
                            p.log_eviction,
                            p.raft,
                        )
                    )
                except Exception as e:
                    ntp = v["value"]["first"]
                    print("Skipping ntp {}: {}".format(model_ntp(ntp), e))

    def invoke(self, arg, from_tty):
        self.print_partitions(parse_shard_arg(arg))


class redpanda_cloud_clients(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda cloud-clients", gdb.COMMAND_USER, gdb.COMPLETE_NONE, True
        )

    def invoke(self, arg, from_tty):
        cpu = parse_shard_arg(arg)
        cpu_list = range(cpus()) if cpu is None else [int(cpu)]
        for i in cpu_list:
            client_pool_ref = find_cloud_storage_clients(i)
            client_pool = cloud_client_pool(client_pool_ref)
            print(f"Client pool on shard {i}")
            print(f"  Available clients ({len(client_pool.pool)}):")
            for c in client_pool.pool:
                print(f"    {c}")
            print(f"  Leased ({len(client_pool.leased)}):")
            for l in client_pool.leased:
                print(f"    {l}")


class iobuf:
    def __init__(self, ref):
        self.size = ref["_size"]
        self.fragments = boost_intrusive_list(ref["_frags"], "hook")

    def __str__(self):
        return f"{{ size: {self.size} }}"


def iobuf_bytes(buf):
    bytes = io.BytesIO()
    for f in buf.fragments:
        used_bytes = f["_used_bytes"]
        buffer = f["_buf"]
        for i in range(used_bytes):
            bytes.write(
                int(buffer["_buffer"][i].format_string(format="u")).to_bytes(
                    1, byteorder="little"
                )
            )
    bytes.seek(0)
    return bytes


class batch_cache_range:
    def __init__(self, ref):
        self.ref = ref
        self.valid = ref["_valid"]
        self.arena = iobuf(ref["_arena"])
        self.offsets = std_vector(ref["_offsets"])
        self.pinned = ref["_pinned"]
        self.size = ref["_size"]

    def __str__(self):
        return f"{{ address: {self.ref.address}, size: {self.size}, pinned: {self.pinned}, valid: {self.valid}, offsets: [{','.join([str(o['_value']) for o in self.offsets])}] }}"


class batch_cache_entry:
    def __init__(self, ref):
        self.ref = ref
        self.range_offset = ref["_range_offset"]
        self.range = batch_cache_range(ref["_range"]["_ptr"].dereference())

    def header(self):
        bytes = iobuf_bytes(self.range.arena)
        # seek to given offset
        bytes.seek(self.range_offset)
        reader = Reader(bytes)
        return {
            "header_crc": reader.read_uint32(),
            "size": reader.read_int32(),
            "base_offset": reader.read_int64(),
            "type": reader.read_int8(),
            "crc": reader.read_int32(),
            "attrs": reader.read_int16(),
            "last_offset_delta": reader.read_int32(),
            "first_ts": reader.read_int64(),
            "max_ts": reader.read_int64(),
            "producer_id": reader.read_int64(),
            "producer_epoch": reader.read_int16(),
            "base_sequence": reader.read_int32(),
            "record_count": reader.read_int32(),
            "term": reader.read_int64(),
        }


class redpanda_batch_cache(gdb.Command):
    """Prints content of redpanda batch cache"""

    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda batch_cache", gdb.COMMAND_USER, gdb.COMPLETE_COMMAND
        )

    def get_log(self, ns, topic, partition):
        for ntp, log in find_logs():
            m_ntp = model_ntp(ntp)
            if (
                str(m_ntp.namespace()).strip('"') == ns
                and str(m_ntp.topic()).strip('"') == topic
                and partition == str(m_ntp.partition())
            ):
                return log
        return None

    def invoke(self, arg, from_tty):
        ns, tp, partition = arg.split("/")
        print(f"# Batch cache for {ns}/{tp}/{partition}")
        log = self.get_log(ns, tp, partition)
        for s in log.segments():
            for k, v in s.batch_cache_index():
                range_offset = k["_value"]
                entry = batch_cache_entry(v)
                print(f"o: {range_offset}, header: {entry.header()}")


class redpanda_small_objects(gdb.Command):
    """List live objects from one of the seastar allocator's small pools

    The pool is selected with the `-o|--object-size` flag. Results are paginated by
    default as there can be millions of objects. Default page size is 20.
    To list a certain page, use the `-p|--page` flag. To find out the number of
    total objects and pages, use `--summarize`.
    To sample random pages, use `--random-page`.

    If objects have a vtable, its type is resolved and this will appear in the
    listing.

    Note that to reach a certain page, the command has to traverse the memory
    spans belonging to the pool linearly, until the desired range of object is
    found. This can take a long time for well populated pools. To speed this
    up, the span iterator is saved and reused when possible. This caching can
    only be exploited withing the same pool and only with monotonically
    increasing pages.

    For usage see: redpanda small-objects --help

    Examples:

    (gdb) redpanda small-objects -o 32 --summarize
    number of objects: 60196912
    page size        : 20
    number of pages  : 3009845

    (gdb) redpanda small-objects -o 32 -p 100
    page 100: 2000-2019
    [2000] 0x635002ecba00
    [2001] 0x635002ecba20
    [2002] 0x635002ecba40
    [2003] 0x635002ecba60
    [2004] 0x635002ecba80
    [2005] 0x635002ecbaa0
    [2006] 0x635002ecbac0
    [2007] 0x635002ecbae0
    [2008] 0x635002ecbb00
    [2009] 0x635002ecbb20
    [2010] 0x635002ecbb40
    [2011] 0x635002ecbb60
    [2012] 0x635002ecbb80
    [2013] 0x635002ecbba0
    [2014] 0x635002ecbbc0
    [2015] 0x635002ecbbe0
    [2016] 0x635002ecbc00
    [2017] 0x635002ecbc20
    [2018] 0x635002ecbc40
    [2019] 0x635002ecbc60
    """

    class small_object_iterator:
        def __init__(self, small_pool, resolve_symbols):
            self._small_pool = small_pool
            self._resolve_symbols = resolve_symbols

            self._text_start, self._text_end = get_text_range()
            self._vptr_type = gdb.lookup_type("uintptr_t").pointer()
            self._free_object_ptr = gdb.lookup_type("void").pointer().pointer()
            self._page_size = int(gdb.parse_and_eval("'seastar::memory::page_size'"))
            self._free_in_pool = set()
            self._free_in_span = set()

            pool_next_free = self._small_pool["_free"]
            while pool_next_free:
                self._free_in_pool.add(int(pool_next_free))
                pool_next_free = pool_next_free.reinterpret_cast(
                    self._free_object_ptr
                ).dereference()

            self._span_it = iter(spans())
            self._obj_it = iter([])  # initialize to exhausted iterator

        def _next_span(self):
            # Let any StopIteration bubble up, as it signals we are done with
            # all spans.
            span = next(self._span_it)
            while span.pool() != self._small_pool.address:
                span = next(self._span_it)

            self._free_in_span = set()
            span_start = int(span.start)
            span_end = int(span_start + span.size() * self._page_size)

            # span's free list
            span_next_free = span.page["freelist"]
            while span_next_free:
                self._free_in_span.add(int(span_next_free))
                span_next_free = span_next_free.reinterpret_cast(
                    self._free_object_ptr
                ).dereference()

            return span_start, span_end

        def _next_obj(self):
            try:
                return next(self._obj_it)
            except StopIteration:
                # Don't call self._next_span() here as it might throw another StopIteration.
                pass

            span_start, span_end = self._next_span()
            self._obj_it = iter(
                range(span_start, span_end, int(self._small_pool["_object_size"]))
            )
            return next(self._obj_it)

        def __next__(self):
            obj = self._next_obj()
            while obj in self._free_in_span or obj in self._free_in_pool:
                obj = self._next_obj()

            if self._resolve_symbols:
                addr = gdb.Value(obj).reinterpret_cast(self._vptr_type).dereference()
                if addr >= self._text_start and addr <= self._text_end:
                    return (obj, resolve(addr))
                else:
                    return (obj, None)
            else:
                return (obj, None)

        def __iter__(self):
            return self

    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda small-objects", gdb.COMMAND_USER, gdb.COMPLETE_COMMAND
        )

        self._parser = None
        self._iterator = None
        self._last_pos = 0
        self._last_object_size = None

    @staticmethod
    def get_object_sizes():
        cpu_mem = gdb.parse_and_eval("'seastar::memory::cpu_mem'")
        small_pools = cpu_mem["small_pools"]
        nr = int(small_pools["nr_small_pools"])
        return [int(small_pools["_u"]["a"][i]["_object_size"]) for i in range(nr)]

    @staticmethod
    def find_small_pool(object_size):
        cpu_mem = gdb.parse_and_eval("'seastar::memory::cpu_mem'")
        small_pools = cpu_mem["small_pools"]
        nr = int(small_pools["nr_small_pools"])
        for i in range(nr):
            sp = small_pools["_u"]["a"][i]
            if object_size == int(sp["_object_size"]):
                return sp

        return None

    def init_parser(self):
        parser = argparse.ArgumentParser(description="redpanda small-objects")
        parser.add_argument(
            "-o",
            "--object-size",
            action="store",
            type=int,
            required=True,
            help="Object size, valid sizes are: {}".format(
                redpanda_small_objects.get_object_sizes()
            ),
        )
        parser.add_argument(
            "-p", "--page", action="store", type=int, default=0, help="Page to show."
        )
        parser.add_argument(
            "-s",
            "--page-size",
            action="store",
            type=int,
            default=20,
            help="Number of objects in a page. A page size of 0 turns off paging.",
        )
        parser.add_argument(
            "--random-page", action="store_true", help="Show a random page."
        )
        parser.add_argument(
            "--summarize",
            action="store_true",
            help="Print the number of objects and pages in the pool.",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Print additional details on what is going on.",
        )

        self._parser = parser

    def get_objects(
        self, small_pool, offset=0, count=0, resolve_symbols=False, verbose=False
    ):
        if (
            self._last_object_size != int(small_pool["_object_size"])
            or offset < self._last_pos
        ):
            self._last_pos = 0
            self._iterator = redpanda_small_objects.small_object_iterator(
                small_pool, resolve_symbols
            )

        skip = offset - self._last_pos
        if verbose:
            gdb.write(
                "get_objects(): offset={}, count={}, last_pos={}, skip={}\n".format(
                    offset, count, self._last_pos, skip
                )
            )

        for _ in range(skip):
            next(self._iterator)

        if count:
            objects = []
            for _ in range(count):
                objects.append(next(self._iterator))
        else:
            objects = list(self._iterator)

        self._last_pos += skip
        self._last_pos += len(objects)

        return objects

    def invoke(self, arg, from_tty):
        if self._parser is None:
            self.init_parser()

        try:
            args = self._parser.parse_args(arg.split())
        except SystemExit:
            return

        small_pool = redpanda_small_objects.find_small_pool(args.object_size)
        if small_pool is None:
            raise ValueError(
                "{} is not a valid object size for any small pools, valid object sizes are: {}",
                redpanda_small_objects.get_object_sizes(),
            )

        if args.summarize:
            if self._last_object_size != args.object_size:
                if args.verbose:
                    gdb.write(
                        "Object size changed ({} -> {}), scanning pool.\n".format(
                            self._last_object_size, args.object_size
                        )
                    )
                self._num_objects = len(
                    self.get_objects(small_pool, verbose=args.verbose)
                )
                self._last_object_size = args.object_size
            gdb.write(
                "number of objects: {}\n"
                "page size        : {}\n"
                "number of pages  : {}\n".format(
                    self._num_objects,
                    args.page_size,
                    int(self._num_objects / args.page_size),
                )
            )
            return

        if args.random_page:
            if self._last_object_size != args.object_size:
                if args.verbose:
                    gdb.write(
                        "Object size changed ({} -> {}), scanning pool.\n".format(
                            self._last_object_size, args.object_size
                        )
                    )
                self._num_objects = len(
                    self.get_objects(small_pool, verbose=args.verbose)
                )
                self._last_object_size = args.object_size
            page = random.randint(0, int(self._num_objects / args.page_size) - 1)
        else:
            page = args.page

        offset = page * args.page_size
        gdb.write("page {}: {}-{}\n".format(page, offset, offset + args.page_size - 1))
        for i, (obj, sym) in enumerate(
            self.get_objects(
                small_pool,
                offset,
                args.page_size,
                resolve_symbols=True,
                verbose=args.verbose,
            )
        ):
            if sym is None:
                sym_text = ""
            else:
                sym_text = sym
            gdb.write("[{}] 0x{:x} {}\n".format(offset + i, obj, sym_text))


class redpanda_task_histogram(gdb.Command):
    """Print a histogram of the virtual objects found in memory.

    Sample the virtual objects in memory and create a histogram with the results.
    By default up to 20000 samples will be collected and the top 30 items will
    be shown. The number of collected samples, as well as number of items shown
    can be customized by command line arguments. The sampling can also be
    constrained to objects of a certain size. For more details invoke:

        redpanda task_histogram --help

    Example:
     12280: 0x4bc5878 vtable for seastar::file_data_source_impl + 16
      9352: 0x4be2cf0 vtable for seastar::continuation<seastar::future<seasta...
      9352: 0x4bc59a0 vtable for seastar::continuation<seastar::future<seasta...
     (1)    (2)       (3)

     Where:
     (1): Number of objects of this type.
     (2): The address of the class's vtable.
     (3): The name of the class's vtable symbol.
    """

    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda task_histogram", gdb.COMMAND_USER, gdb.COMPLETE_COMMAND
        )

    def invoke(self, arg, from_tty):
        parser = argparse.ArgumentParser(description="redpanda task_histogram")
        parser.add_argument(
            "-m",
            "--samples",
            action="store",
            type=int,
            default=20000,
            help="The number of samples to collect. Defaults to 20000. Set to 0 to sample all objects. Ignored when `--all` is used."
            " Note that due to this limit being checked only after scanning an entire page, in practice it will always be overshot.",
        )
        parser.add_argument(
            "-c",
            "--count",
            action="store",
            type=int,
            default=30,
            help="Show only the top COUNT elements of the histogram. Defaults to 30. Set to 0 to show all items. Ignored when `--all` is used.",
        )
        parser.add_argument(
            "-a",
            "--all",
            action="store_true",
            default=False,
            help="Sample all pages and show all results. Equivalent to -m=0 -c=0.",
        )
        parser.add_argument(
            "-s",
            "--size",
            action="store",
            default=0,
            help="The size of objects to sample. When set, only objects of this size will be sampled. A size of 0 (the default value) means no size restrictions.",
        )
        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        size = args.size
        cpu_mem = gdb.parse_and_eval("'seastar::memory::cpu_mem'")
        page_size = int(gdb.parse_and_eval("'seastar::memory::page_size'"))
        mem_start = cpu_mem["memory"]

        vptr_type = gdb.lookup_type("uintptr_t").pointer()

        pages = cpu_mem["pages"]
        nr_pages = int(cpu_mem["nr_pages"])
        page_samples = (
            range(0, nr_pages)
            if args.all
            else random.sample(range(0, nr_pages), nr_pages)
        )

        text_start, text_end = get_text_range()

        sc = span_checker()
        vptr_count = defaultdict(int)
        scanned_pages = 0
        for idx in page_samples:
            span = sc.get_span(mem_start + idx * page_size)
            if not span or span.index != idx or not span.is_small():
                continue
            pool = span.pool()
            if int(pool.dereference()["_object_size"]) != size and size != 0:
                continue
            scanned_pages += 1
            objsize = size if size != 0 else int(pool.dereference()["_object_size"])
            span_size = span.used_span_size() * page_size
            for idx2 in range(0, int(span_size / objsize)):
                obj_addr = span.start + idx2 * objsize
                addr = gdb.Value(obj_addr).reinterpret_cast(vptr_type).dereference()
                if addr >= text_start and addr <= text_end:
                    vptr_count[int(addr)] += 1
            if args.all or args.samples == 0:
                continue
            if scanned_pages >= args.samples or len(vptr_count) >= args.samples:
                break

        sorted_counts = sorted(vptr_count.items(), key=lambda e: -e[1])
        to_show = (
            sorted_counts
            if args.all or args.count == 0
            else sorted_counts[: args.count]
        )
        for vptr, count in to_show:
            sym = resolve(vptr)
            if sym:
                gdb.write("%10d: 0x%x %s\n" % (count, vptr, sym))


class redpanda_task_queues(gdb.Command):
    """Print a summary of the reactor's task queues.

    Example:
       id name                             shares  tasks
     A 00 "main"                           1000.00 4
       01 "atexit"                         1000.00 0
       02 "streaming"                       200.00 0
     A 03 "compaction"                      171.51 1
       04 "mem_compaction"                 1000.00 0
    *A 05 "statement"                      1000.00 2
       06 "memtable"                          8.02 0
       07 "memtable_to_cache"               200.00 0

    Where:
        * id: seastar::reactor::task_queue::_id
        * name: seastar::reactor::task_queue::_name
        * shares: seastar::reactor::task_queue::_shares
        * tasks: seastar::reactor::task_queue::_q.size()
        * A: seastar::reactor::task_queue::_active == true
        * *: seastar::reactor::task_queue::_current == true
    """

    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda task-queues", gdb.COMMAND_USER, gdb.COMPLETE_NONE, True
        )

    @staticmethod
    def _active(a):
        if a:
            return "A"
        return " "

    @staticmethod
    def _current(c):
        if c:
            return "*"
        return " "

    def invoke(self, arg, for_tty):
        current_sg = gdb.parse_and_eval(
            "(seastar::scheduling_group) 'seastar::internal::current_scheduling_group_ptr()::sg'"
        )
        gdb.write("   {:2} {:32} {:7} {}\n".format("id", "name", "shares", "tasks"))
        for tq in get_local_task_queues():
            gdb.write(
                "{}{} {:02} {:32} {:>7.2f} {}\n".format(
                    self._current(current_sg["_id"] == tq["_id"]),
                    self._active(bool(tq["_active"])),
                    int(tq["_id"]),
                    str(tq["_name"]),
                    float(tq["_shares"]),
                    len(seastar_circular_buffer(tq["_q"])),
                )
            )


class redpanda_smp_queues(gdb.Command):
    """Summarize the shard's outgoing smp queues.

    The summary takes the form of a histogram. Example:

        (gdb) redpanda smp-queues
            10747 17 ->  3 ++++++++++++++++++++++++++++++++++++++++
              721 17 -> 19 ++
              247 17 -> 20 +
              233 17 -> 10 +
              210 17 -> 14 +
              205 17 ->  4 +
              204 17 ->  5 +
              198 17 -> 16 +
              197 17 ->  6 +
              189 17 -> 11 +
              181 17 ->  1 +
              179 17 -> 13 +
              176 17 ->  2 +
              173 17 ->  0 +
              163 17 ->  8 +
                1 17 ->  9 +

    Each line has the following format

        count from -> to ++++

    Where:
        count: the number of items in the queue;
        from: the shard, from which the message was sent (this shard);
        to: the shard, to which the message is sent;
        ++++: visual illustration of the relative size of this queue;
    """

    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda smp-queues", gdb.COMMAND_USER, gdb.COMPLETE_COMMAND
        )
        self.queues = set()

    def _init(self):
        qs = std_unique_ptr(gdb.parse_and_eval("seastar::smp::_qs")).get()
        for i in range(cpus()):
            for j in range(cpus()):
                self.queues.add(int(qs[i][j].address))
        self._queue_type = gdb.lookup_type("seastar::smp_message_queue").pointer()
        self._ptr_type = gdb.lookup_type("uintptr_t").pointer()

    def invoke(self, arg, from_tty):
        if not self.queues:
            self._init()

        def formatter(q):
            a, b = q
            return "{:2} -> {:2}".format(a, b)

        h = histogram(formatter=formatter)
        known_vptrs = dict()

        for obj, vptr in find_vptrs():
            obj = int(obj)
            vptr = int(vptr)

            if not vptr in known_vptrs:
                name = resolve(
                    vptr,
                    startswith="vtable for seastar::smp_message_queue::async_work_item",
                )
                if name:
                    known_vptrs[vptr] = None
                else:
                    continue

            offset = known_vptrs[vptr]

            if offset is None:
                q = None
                ptr_meta = redpanda_ptr.analyze(obj)
                for offset in range(0, ptr_meta.size, self._ptr_type.sizeof):
                    ptr = int(
                        gdb.Value(obj + offset)
                        .reinterpret_cast(self._ptr_type)
                        .dereference()
                    )
                    if ptr in self.queues:
                        q = (
                            gdb.Value(ptr)
                            .reinterpret_cast(self._queue_type)
                            .dereference()
                        )
                        break
                known_vptrs[vptr] = offset
                if q is None:
                    continue
            else:
                ptr = int(
                    gdb.Value(obj + offset)
                    .reinterpret_cast(self._ptr_type)
                    .dereference()
                )
                q = gdb.Value(ptr).reinterpret_cast(self._queue_type).dereference()

            a = int(q["_completed"]["remote"]["_id"])
            b = int(q["_pending"]["remote"]["_id"])
            h[(a, b)] += 1

        gdb.write("{}\n".format(h))


class redpanda_tasks(gdb.Command):
    """Prints contents of reactor pending tasks queue.

    Example:
    (gdb) redpanda tasks
    (task*) 0x60017d8c7f88  _ZTV12continuationIZN6futureIJEE12then_wrappedIZN17smp_message_queu...
    (task*) 0x60019a391730  _ZTV12continuationIZN6futureIJEE12then_wrappedIZNS1_16handle_except...
    (task*) 0x60018fac2208  vtable for lambda_task<yield()::{lambda()#1}> + 16
    (task*) 0x60016e8b7428  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...
    (task*) 0x60017e5bece8  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...
    (task*) 0x60017e7f8aa0  _ZTV12continuationIZN6futureIJEE12then_wrappedIZNS1_16handle_except...
    (task*) 0x60018fac21e0  vtable for lambda_task<yield()::{lambda()#1}> + 16
    (task*) 0x60016e8b7540  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...
    (task*) 0x600174c34d58  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...

            ^               ^
            |               |
            |               '------------ symbol name for task's vtable pointer
            '---------------------------- task pointer
    """

    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda tasks", gdb.COMMAND_USER, gdb.COMPLETE_NONE, True
        )

    def invoke(self, arg, for_tty):
        vptr_type = gdb.lookup_type("uintptr_t").pointer()
        for ptr in get_local_tasks():
            vptr = int(ptr.reinterpret_cast(vptr_type).dereference())
            gdb.write("(task*) 0x%x  %s\n" % (ptr, resolve(vptr)))


class redpanda(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda", gdb.COMMAND_USER, gdb.COMPLETE_COMMAND, True
        )


class sstring_printer(gdb.printing.PrettyPrinter):
    "print an sstring"

    def __init__(self, val):
        self.val = val

    def to_string(self):
        if self.val["u"]["internal"]["size"] >= 0:
            array = self.val["u"]["internal"]["str"]
            len = int(self.val["u"]["internal"]["size"])
            return "".join([chr(array[x]) for x in range(len)])
        else:
            # TODO: looks broken for external?
            return self.val["u"]["external"]["str"]

    def display_hint(self):
        return "string"


class model_ntp_printer(gdb.printing.PrettyPrinter):
    "print a model::ntp"

    def __init__(self, val):
        self.val = val

    def to_string(self):
        ns = self.val["ns"]["_value"]
        topic = self.val["tp"]["topic"]["_value"]
        partition = self.val["tp"]["partition"]["_value"]
        return f"{{{ns}}}.{{{topic}}}.{{{partition}}}"

    def display_hint(self):
        return "model::ntp"


def build_pretty_printer():
    pp = gdb.printing.RegexpCollectionPrettyPrinter("redpanda")
    pp.add_printer("sstring", r"^seastar::basic_sstring<char,.*>$", sstring_printer)
    pp.add_printer("model::ntp", r"^model::ntp$", model_ntp_printer)
    return pp


gdb.printing.register_pretty_printer(
    gdb.current_objfile(), build_pretty_printer(), replace=True
)


class TreeNode(object):
    def __init__(self, key):
        self.key = key
        self.children_by_key = {}

    def get_or_add(self, key):
        node = self.children_by_key.get(key, None)
        if not node:
            node = self.__class__(key)
            self.add(node)
        return node

    def add(self, node):
        self.children_by_key[node.key] = node

    def squash_child(self):
        assert self.has_only_one_child()
        self.children_by_key = next(iter(self.children)).children_by_key

    @property
    def children(self):
        return self.children_by_key.values()

    def has_only_one_child(self):
        return len(self.children_by_key) == 1

    def has_children(self):
        return bool(self.children_by_key)

    def remove_all(self):
        self.children_by_key.clear()


class ProfNode(TreeNode):
    def __init__(self, key):
        super(ProfNode, self).__init__(key)
        self.size = 0
        self.count = 0
        self.tail = []

    @property
    def attributes(self):
        return {"size": self.size, "count": self.count}


def collapse_similar(node):
    while node.has_only_one_child():
        child = next(iter(node.children))
        if node.attributes == child.attributes:
            node.squash_child()
            node.tail.append(child.key)
        else:
            break

    for child in node.children:
        collapse_similar(child)


def strip_level(node, level):
    if level <= 0:
        node.remove_all()
    else:
        for child in node.children:
            strip_level(child, level - 1)


def print_tree(
    root_node,
    formatter=attrgetter("key"),
    order_by=attrgetter("key"),
    printer=sys.stdout.write,
    node_filter=None,
):
    def print_node(node, is_last_history):
        stems = (" |   ", "     ")
        branches = (" |-- ", " \\-- ")

        label_lines = formatter(node).rstrip("\n").split("\n")
        prefix_without_branch = "".join(map(stems.__getitem__, is_last_history[:-1]))

        if is_last_history:
            printer(prefix_without_branch)
            printer(branches[is_last_history[-1]])
        printer("%s\n" % label_lines[0])

        for line in label_lines[1:]:
            printer("".join(map(stems.__getitem__, is_last_history)))
            printer("%s\n" % line)

        children = sorted(filter(node_filter, node.children), key=order_by)
        if children:
            for child in children[:-1]:
                print_node(child, is_last_history + [False])
            print_node(children[-1], is_last_history + [True])

        is_last = not is_last_history or is_last_history[-1]
        if not is_last:
            printer("%s%s\n" % (prefix_without_branch, stems[False]))

    if not node_filter or node_filter(root_node):
        print_node(root_node, [])


class redpanda_heapprof(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda heapprof", gdb.COMMAND_USER, gdb.COMPLETE_COMMAND
        )

    def invoke(self, arg, from_tty):
        parser = argparse.ArgumentParser(description="redpanda heapprof")
        parser.add_argument(
            "-G",
            "--inverted",
            action="store_true",
            help="Compute caller-first profile instead of callee-first",
        )
        parser.add_argument(
            "-a",
            "--addresses",
            action="store_true",
            help="Show raw addresses before resolved symbol names",
        )
        parser.add_argument(
            "--no-symbols", action="store_true", help="Show only raw addresses"
        )
        parser.add_argument(
            "--flame",
            action="store_true",
            help="Write flamegraph data to heapprof.stacks instead of showing the profile",
        )
        parser.add_argument(
            "--min",
            action="store",
            type=int,
            default=0,
            help="Drop branches allocating less than given amount",
        )
        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        root = ProfNode(None)
        cpu_mem = gdb.parse_and_eval("'seastar::memory::cpu_mem'")
        site = cpu_mem["alloc_site_list_head"]

        shared_objects = std_vector(gdb.parse_and_eval("'seastar::shared_objects'"))

        while site:
            size = int(site["size"])
            count = int(site["count"])
            if size:
                n = root
                n.size += size
                n.count += count
                bt = site["backtrace"]
                addresses = list(
                    int(f["addr"]) for f in seastar_static_vector(bt["_frames"])
                )
                addresses.pop(0)  # drop memory::get_backtrace()
                if args.inverted:
                    seq = reversed(addresses)
                else:
                    seq = addresses
                for addr in seq:
                    n = n.get_or_add(addr)
                    n.size += size
                    n.count += count
            site = site["next"]

        def resolve_relative(addr):
            for so in shared_objects:
                sym = resolve(addr + int(so["begin"]))
                if sym:
                    return sym
            return None

        def resolver(addr):
            if args.no_symbols:
                return "0x%x" % addr
            if args.addresses:
                return "0x%x %s" % (addr, resolve_relative(addr) or "")
            return resolve_relative(addr) or ("0x%x" % addr)

        if args.flame:
            file_name = "heapprof.stacks"
            with open(file_name, "w") as out:
                trace = list()

                def print_node(n):
                    if n.key:
                        trace.append(n.key)
                        trace.extend(n.tail)
                    for c in n.children:
                        print_node(c)
                    if not n.has_children():
                        out.write(
                            "%s %d\n"
                            % (
                                ";".join(
                                    map(lambda x: "%s" % (x), map(resolver, trace))
                                ),
                                n.size,
                            )
                        )
                    if n.key:
                        del trace[-1 - len(n.tail) :]

                print_node(root)
            gdb.write("Wrote %s\n" % (file_name))
        else:

            def node_formatter(n):
                if n.key is None:
                    name = "All"
                else:
                    name = resolver(n.key)
                return "%s (%d, #%d)\n%s" % (
                    name,
                    n.size,
                    n.count,
                    "\n".join(map(resolver, n.tail)),
                )

            def node_filter(n):
                return n.size >= args.min

            collapse_similar(root)
            print_tree(
                root,
                formatter=node_formatter,
                order_by=lambda n: -n.size,
                node_filter=node_filter,
                printer=gdb.write,
            )


def reactors():
    orig = gdb.selected_thread()
    for t in gdb.selected_inferior().threads():
        t.switch()
        reactor = gdb.parse_and_eval("'seastar'::local_engine")
        if reactor:
            yield reactor.dereference()
    orig.switch()


class task_symbol_matcher:
    def __init__(self):
        self._coro_pattern = re.compile(r"\)( \[clone \.\w+\])?$")

        # List of whitelisted symbol names. Each symbol is a tuple, where each
        # element is a component of the name, the last element being the class
        # name itself.
        # We can't just merge them as `info symbol` might return mangled names too.
        self._whitelist = task_symbol_matcher._make_symbol_matchers(
            [
                ("seastar", "continuation"),
                (
                    "seastar",
                    "future",
                    "thread_wake_task",
                ),  # backward compatibility with older versions
                ("seastar", "(anonymous namespace)", "thread_wake_task"),
                ("seastar", "thread_context"),
                ("seastar", "internal", "do_until_state"),
                ("seastar", "internal", "do_with_state"),
                ("seastar", "internal", "do_for_each_state"),
                ("seastar", "parallel_for_each_state"),
                ("seastar", "internal", "repeat_until_value_state"),
                ("seastar", "internal", "repeater"),
                ("seastar", "internal", "when_all_state"),
                ("seastar", "internal", "when_all_state_component"),
                ("seastar", "internal", "coroutine_traits_base", "promise_type"),
                ("seastar", "lambda_task"),
                ("seastar", "smp_message_queue", "async_work_item"),
            ]
        )

    @staticmethod
    def _make_symbol_matchers(symbol_specs):
        return list(map(task_symbol_matcher._make_symbol_matcher, symbol_specs))

    @staticmethod
    def _make_symbol_matcher(symbol_spec):
        unmangled_prefix = "vtable for {}".format("::".join(symbol_spec))

        def matches_symbol(name):
            if name.startswith(unmangled_prefix):
                return True

            try:
                positions = [name.index(part) for part in symbol_spec]
                return sorted(positions) == positions
            except ValueError:
                return False

        return matches_symbol

    def __call__(self, name):
        name = name.strip()
        if re.search(self._coro_pattern, name) is not None:
            return True

        for matcher in self._whitelist:
            if matcher(name):
                return True
        return False


class pointer_metadata(object):
    def __init__(self, ptr, *args):
        print(f">>> args={args}")
        if isinstance(args[0], gdb.InferiorThread):
            self._init_seastar_ptr(ptr, *args)
        else:
            self._init_generic_ptr(ptr, *args)

    def _init_seastar_ptr(self, ptr, thread):
        self.ptr = ptr
        self.thread = thread
        self._is_containing_page_free = False
        self.is_small = False
        self.is_live = False
        self.size = 0
        self.offset_in_object = 0

    def _init_generic_ptr(self, ptr, speculative_size):
        self.ptr = ptr
        self.thread = None
        self._is_containing_page_free = None
        self.is_small = None
        self.is_live = None
        self.size = speculative_size
        self.offset_in_object = 0

    def is_managed_by_seastar(self):
        return self.thread is not None

    @property
    def is_containing_page_free(self):
        return self._is_containing_page_free

    def mark_free(self):
        self._is_containing_page_free = True
        self._is_live = False

    @property
    def obj_ptr(self):
        return self.ptr - self.offset_in_object

    def __str__(self):
        if not self.is_managed_by_seastar():
            return "0x{:x} (default allocator)".format(self.ptr)

        msg = "thread %d" % self.thread.num

        if self.is_containing_page_free:
            msg += ", page is free"
            return msg

        if self.is_small:
            msg += ", small (size <= %d)" % self.size
        else:
            msg += ", large (size=%d)" % self.size

        if self.is_live:
            msg += ", live (0x%x +%d)" % (self.obj_ptr, self.offset_in_object)
        else:
            msg += ", free (0x%x +%d)" % (self.obj_ptr, self.offset_in_object)

        return msg


def has_reactor():
    if gdb.parse_and_eval("'seastar'::local_engine"):
        return True
    return False


@functools.cache
def _vptr_type():
    return gdb.lookup_type("uintptr_t").pointer()


def reactor_threads():
    orig = gdb.selected_thread()
    for t in gdb.selected_inferior().threads():
        t.switch()
        if has_reactor():
            yield t
    orig.switch()


def get_seastar_memory_start_and_size():
    cpu_mem = gdb.parse_and_eval("'seastar::memory::cpu_mem'")
    print("cpu_mem = {}".format(cpu_mem))
    page_size = int(gdb.parse_and_eval("'seastar::memory::page_size'"))
    print("page_size = {}".format(page_size))
    total_mem = int(cpu_mem["nr_pages"]) * page_size
    start = int(cpu_mem["memory"])
    return start, total_mem


def seastar_memory_layout():
    results = []
    for t in reactor_threads():
        start, total_mem = get_seastar_memory_start_and_size()
        results.append((t, start, total_mem))
    return results


class redpanda_ptr(gdb.Command):
    _is_seastar_allocator_used = None

    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda ptr", gdb.COMMAND_USER, gdb.COMPLETE_COMMAND
        )

    @staticmethod
    def is_seastar_allocator_used():
        if redpanda_ptr._is_seastar_allocator_used is not None:
            return redpanda_ptr._is_seastar_allocator_used

        try:
            cpu_mem = gdb.parse_and_eval("'seastar::memory::cpu_mem'")

            redpanda_ptr._is_seastar_allocator_used = True
            return True
        except:
            redpanda_ptr._is_seastar_allocator_used = False
            return False

    @staticmethod
    def _do_analyze(ptr):
        owning_thread = None
        for t, start, size in seastar_memory_layout():
            if ptr >= start and ptr < start + size:
                owning_thread = t
                break

        ptr_meta = pointer_metadata(ptr, owning_thread)

        if not owning_thread:
            return ptr_meta

        owning_thread.switch()

        cpu_mem = gdb.parse_and_eval("'seastar::memory::cpu_mem'")
        page_size = int(gdb.parse_and_eval("'seastar::memory::page_size'"))
        offset = ptr - int(cpu_mem["memory"])
        ptr_page_idx = offset / page_size
        pages = cpu_mem["pages"]
        page = pages[ptr_page_idx]

        span = span_checker().get_span(ptr)
        offset_in_span = ptr - span.start
        if offset_in_span >= span.used_span_size() * page_size:
            ptr_meta.mark_free()
        elif span.is_small():
            pool = span.pool()
            object_size = int(pool["_object_size"])
            ptr_meta.size = object_size
            ptr_meta.is_small = True
            offset_in_object = offset_in_span % object_size
            free_object_ptr = gdb.lookup_type("void").pointer().pointer()
            char_ptr = gdb.lookup_type("char").pointer()
            # pool's free list
            next_free = pool["_free"]
            free = False
            while next_free:
                if (
                    ptr >= next_free
                    and ptr < next_free.reinterpret_cast(char_ptr) + object_size
                ):
                    free = True
                    break
                next_free = next_free.reinterpret_cast(free_object_ptr).dereference()
            if not free:
                # span's free list
                first_page_in_span = span.page
                next_free = first_page_in_span["freelist"]
                while next_free:
                    if (
                        ptr >= next_free
                        and ptr < next_free.reinterpret_cast(char_ptr) + object_size
                    ):
                        free = True
                        break
                    next_free = next_free.reinterpret_cast(
                        free_object_ptr
                    ).dereference()
            ptr_meta.offset_in_object = offset_in_object
            ptr_meta.is_live = not free
        else:
            ptr_meta.is_small = False
            ptr_meta.is_live = not span.is_free()
            ptr_meta.size = span.size() * page_size
            ptr_meta.offset_in_object = ptr - span.start

        return ptr_meta

    @staticmethod
    def analyze(ptr):
        orig = gdb.selected_thread()
        try:
            return redpanda_ptr._do_analyze(ptr)
        finally:
            orig.switch()

    def invoke(self, arg, from_tty):
        ptr = int(gdb.parse_and_eval(arg))

        ptr_meta = self.analyze(ptr)

        gdb.write("{}\n".format(str(ptr_meta)))


def switch_to_shard(shard):
    for r in reactors():
        if int(r["_id"]) == shard:
            return


def find_objects(mem_start, mem_size, value, size_selector="g", only_live=True):
    for line in gdb.execute(
        "find/%s 0x%x, +0x%x, 0x%x" % (size_selector, mem_start, mem_size, value),
        to_string=True,
    ).split("\n"):
        if line.startswith("0x"):
            ptr_meta = redpanda_ptr.analyze(int(line, base=16))
            if not only_live or ptr_meta.is_live:
                yield ptr_meta


class redpanda_find(gdb.Command):
    """Finds live objects on seastar heap of current shard which contain given value.
    Prints results in 'redpanda ptr' format.

    See `redpanda find --help` for more details on usage.

    Example:

      (gdb) redpanda find 0x600005321900
      thread 1, small (size <= 512), live (0x6000000f3800 +48)
      thread 1, small (size <= 56), live (0x6000008a1230 +32)
    """

    _size_char_to_size = {
        "b": 8,
        "h": 16,
        "w": 32,
        "g": 64,
    }

    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda find", gdb.COMMAND_USER, gdb.COMPLETE_NONE, True
        )

    @staticmethod
    def find(value, size_selector="g", value_range=0, find_all=False, only_live=True):
        step = int(redpanda_find._size_char_to_size[size_selector] / 8)
        offset = 0
        print(">> find start")
        mem_start, mem_size = get_seastar_memory_start_and_size()
        print(
            ">> find start {mem_start}, {mem_size}".format(
                mem_start=mem_start, mem_size=mem_size
            )
        )
        it = iter(find_objects(mem_start, mem_size, value, size_selector, only_live))

        # Find the first value in the range for which the search has results.
        while offset < value_range:
            try:
                yield next(it), offset
                if not find_all:
                    break
            except StopIteration:
                offset += step
                it = iter(
                    find_objects(
                        mem_start, mem_size, value + offset, size_selector, only_live
                    )
                )

        # List the rest of the results for value.
        try:
            while True:
                yield next(it), offset
        except StopIteration:
            pass

    def invoke(self, arg, for_tty):
        parser = argparse.ArgumentParser(description="redpanda find")
        parser.add_argument(
            "-s",
            "--size",
            action="store",
            choices=["b", "h", "w", "g", "8", "16", "32", "64"],
            default="g",
            help="Size of the searched value."
            " Accepted values are the size expressed in number of bits: 8, 16, 32 and 64."
            " GDB's size classes are also accepted: b(byte), h(half-word), w(word) and g(giant-word)."
            " Defaults to g (64 bits).",
        )
        parser.add_argument(
            "-r",
            "--resolve",
            action="store_true",
            help="Attempt to resolve the first pointer in the found objects as vtable pointer. "
            " If the resolve is successful the vtable pointer as well as the vtable symbol name will be printed in the listing.",
        )
        parser.add_argument(
            "--value-range",
            action="store",
            type=int,
            default=0,
            help="Find a range of values of the specified size."
            " Will start from VALUE, then use SIZE increments until VALUE + VALUE_RANGE is reached, or at least one usage is found."
            " By default only VALUE is searched.",
        )
        parser.add_argument(
            "-a",
            "--find-all",
            action="store_true",
            help="Find all references, don't stop at the first offset which has usages. See --value-range.",
        )
        parser.add_argument(
            "-f",
            "--include-free",
            action="store_true",
            help="Include freed object in the result.",
        )
        parser.add_argument("value", action="store", help="The value to be searched.")

        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        size_arg_to_size_char = {
            "b": "b",
            "8": "b",
            "h": "h",
            "16": "h",
            "w": "w",
            "32": "w",
            "g": "g",
            "64": "g",
        }

        size_char = size_arg_to_size_char[args.size]
        print(f">>> gdb.parse_and_eval(args.value)")
        print(f">>> {gdb.parse_and_eval(args.value)}")
        for ptr_meta, offset in redpanda_find.find(
            int(gdb.parse_and_eval(args.value)),
            size_char,
            args.value_range,
            find_all=args.find_all,
            only_live=(not args.include_free),
        ):
            if args.value_range and offset:
                formatted_offset = "+{}; ".format(offset)
            else:
                formatted_offset = ""
            if args.resolve:
                maybe_vptr = int(
                    gdb.Value(ptr_meta.obj_ptr)
                    .reinterpret_cast(_vptr_type())
                    .dereference()
                )
                symbol = resolve(maybe_vptr, cache=False)
                if symbol is None:
                    gdb.write("{}{}\n".format(formatted_offset, ptr_meta))
                else:
                    gdb.write(
                        "{}{} 0x{:016x} {}\n".format(
                            formatted_offset, ptr_meta, maybe_vptr, symbol
                        )
                    )
            else:
                gdb.write("{}{}\n".format(formatted_offset, ptr_meta))


def align_up(ptr, alignment):
    res = ptr % alignment
    if not res:
        return ptr
    return ptr + alignment - res


class redpanda_fiber(gdb.Command):
    """Walk the continuation chain starting from the given task

    Example (cropped for brevity):
    (gdb) redpanda fiber 0x600016217c80
    [shard  0] #-1 (task*) 0x000060001a305910 0x0000000004aa5260 vtable for seastar::continuation<...> + 16
    [shard  0] #0  (task*) 0x0000600016217c80 0x0000000004aa5288 vtable for seastar::continuation<...> + 16
    [shard  0] #1  (task*) 0x000060000ac42940 0x0000000004aa2aa0 vtable for seastar::continuation<...> + 16
    [shard  0] #2  (task*) 0x0000600023f59a50 0x0000000004ac1b30 vtable for seastar::continuation<...> + 16
     ^          ^          ^                  ^                  ^
    (1)        (2)        (3)                (4)                (5)

    1) Shard the task lives on (continuation chains can spawn multiple shards)
    2) Task index:
        - 0 is the task passed to the command
        - < 0 are tasks this task waits on
        - > 0 are tasks waiting on this task
    3) Pointer to the task object.
    4) Pointer to the task's vtable.
    5) Symbol name of the task's vtable.

    Invoke `redpanda fiber --help` for more information on usage.
    """

    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda fiber", gdb.COMMAND_USER, gdb.COMPLETE_NONE, True
        )
        self._task_symbol_matcher = task_symbol_matcher()
        self._thread_map = None

    def _name_is_on_whitelist(self, name):
        return self._task_symbol_matcher(name)

    def _maybe_log(self, msg, verbose):
        if verbose:
            gdb.write(msg)

    def _probe_pointer(
        self, ptr, scanned_region_size, using_seastar_allocator, verbose
    ):
        """Check if the pointer is a task pointer

        The pattern we are looking for is:
        ptr -> vtable ptr for a symbol that matches our whitelist

        In addition, ptr has to point to an allocation block, managed by
        seastar, that contains a live object.
        """
        # Save work if caller already analyzed the pointer
        if isinstance(ptr, pointer_metadata):
            ptr_meta = ptr
            ptr = ptr.ptr
        else:
            ptr_meta = None

        try:
            maybe_vptr = int(
                gdb.Value(ptr).reinterpret_cast(_vptr_type()).dereference()
            )
            self._maybe_log("\t-> 0x{:016x}\n".format(maybe_vptr), verbose)
        except gdb.MemoryError:
            self._maybe_log("\tNot a pointer\n", verbose)
            return

        resolved_symbol = resolve(maybe_vptr, False)
        if resolved_symbol is None:
            self._maybe_log("\t\tNot a vtable ptr\n", verbose)
            return

        self._maybe_log("\t\t=> {}\n".format(resolved_symbol), verbose)

        if not self._name_is_on_whitelist(resolved_symbol):
            self._maybe_log(
                "\t\t\tSymbol name doesn't match whitelisted symbols\n", verbose
            )
            return

        # The promise object starts on the third `uintptr_t` in the frame.
        # The resume_fn pointer is the first `uintptr_t`.
        # So if the task is a coroutine, we should be able to find the resume function via offsetting by -2.
        # AFAIK both major compilers respect this convention.
        if resolved_symbol.startswith(
            "vtable for seastar::internal::coroutine_traits_base"
        ):
            if block := gdb.block_for_pc(
                (gdb.Value(ptr).cast(_vptr_type()) - 2).dereference()
            ):
                resume = block.function
                resolved_symbol += (
                    f" ({resume.print_name} at {resume.symtab.filename}:{resume.line})"
                )
            else:
                resolved_symbol += f" (unknown coroutine)"

        if using_seastar_allocator:
            if ptr_meta is None:
                ptr_meta = redpanda_ptr.analyze(ptr)
            if not ptr_meta.is_managed_by_seastar() or not ptr_meta.is_live:
                self._maybe_log("\t\t\tNot a live object\n", verbose)
                return
        else:
            ptr_meta = pointer_metadata(ptr, scanned_region_size)

        self._maybe_log("\t\t\tTask found\n", verbose)

        return ptr_meta, maybe_vptr, resolved_symbol

    # Find futures waiting on this task
    def _walk_forward(
        self,
        ptr_meta,
        name,
        i,
        max_depth,
        scanned_region_size,
        using_seastar_allocator,
        verbose,
    ):
        ptr = ptr_meta.ptr

        # thread_context has a self reference in its `_func` field which will be
        # found before the ref to the next task if the whole object is scanned.
        # Exploit that thread_context is a type we can cast to and scan the
        # `_done` field explicitly to avoid that.
        if "thread_context" in name:
            self._maybe_log(
                "Current task is a thread, using its _done field to continue\n", verbose
            )
            thread_context_ptr_type = gdb.lookup_type(
                "seastar::thread_context"
            ).pointer()
            ctxt = (
                gdb.Value(int(ptr_meta.ptr))
                .reinterpret_cast(thread_context_ptr_type)
                .dereference()
            )
            pr = ctxt["_done"]
            region_start = pr.address
            region_end = region_start + pr.type.sizeof
        else:
            region_start = ptr + _vptr_type().sizeof  # ignore our own vtable
            region_end = region_start + (
                ptr_meta.size - ptr_meta.size % _vptr_type().sizeof
            )

        self._maybe_log(
            "Scanning task #{} @ 0x{:016x}: {}\n".format(i, ptr, str(ptr_meta)), verbose
        )

        for it in range(region_start, region_end, _vptr_type().sizeof):
            maybe_tptr = int(gdb.Value(it).reinterpret_cast(_vptr_type()).dereference())
            self._maybe_log(
                "0x{:016x}+0x{:04x} -> 0x{:016x}\n".format(ptr, it - ptr, maybe_tptr),
                verbose,
            )

            res = self._probe_pointer(
                maybe_tptr, scanned_region_size, using_seastar_allocator, verbose
            )

            if res is None:
                continue

            if int(res[0].ptr) == int(ptr):
                self._maybe_log("Rejecting self reference\n", verbose)
                continue

            return res

        return None

    # Find futures waited-on by this task
    def _walk_backward(
        self,
        ptr_meta,
        name,
        i,
        max_depth,
        scanned_region_size,
        using_seastar_allocator,
        verbose,
    ):
        orig = gdb.selected_thread()
        res = None

        # Threads need special handling as they allocate the thread_wait_task object on their stack.
        if "thread_context" in name:
            context = gdb.parse_and_eval(
                "(seastar::thread_context*)0x{:x}".format(ptr_meta.ptr)
            )
            stack_ptr = int(std_unique_ptr(context["_stack"]).get())
            self._maybe_log(
                "Current task is a thread, trying to find the thread_wake_task on its stack: 0x{:x}\n".format(
                    stack_ptr
                ),
                verbose,
            )
            stack_meta = redpanda_ptr.analyze(stack_ptr)
            # stack grows downwards, so walk from end of buffer towards the beginning
            for maybe_tptr in range(
                stack_ptr + stack_meta.size - _vptr_type().sizeof,
                stack_ptr - _vptr_type().sizeof,
                -_vptr_type().sizeof,
            ):
                res = self._probe_pointer(
                    maybe_tptr, scanned_region_size, using_seastar_allocator, verbose
                )
                if res is not None and "thread_wake_task" in res[2]:
                    return res
            return None

        if name.startswith("vtable for seastar::internal::when_all_state"):
            when_all_state_base_ptr_type = gdb.lookup_type(
                "seastar::internal::when_all_state_base"
            ).pointer()
            when_all_state_base = gdb.Value(int(ptr_meta.ptr)).reinterpret_cast(
                when_all_state_base_ptr_type
            )
            ptr = int(when_all_state_base["_continuation"])
            self._maybe_log(
                "Current task is a when_all_state, looking for references to its continuation field 0x{:x}\n".format(
                    ptr
                ),
                verbose,
            )
            return self._probe_pointer(
                ptr, scanned_region_size, using_seastar_allocator, verbose
            )

        # Async work items will have references on the remote shard, so we first
        # have to find out which is the remote shard and then switch to it.
        # Have to match the start of the name, as async_work_item kicks off
        # continuation and so it will be found in the name of those lambdas.
        if name.startswith("vtable for seastar::smp_message_queue::async_work_item"):
            self._maybe_log(
                "Current task is a async work item, trying to deduce the remote shard\n",
                verbose,
            )
            smp_mmessage_queue_ptr_type = gdb.lookup_type(
                "seastar::smp_message_queue"
            ).pointer()
            work_item_type = gdb.lookup_type("seastar::smp_message_queue::work_item")
            # Casts to templates with lambda template arguments just don't work.
            # We know the offset of the message queue reference in
            # async_work_item (first field) so we calculate and cast a pointer to it.
            q_ptr = align_up(
                int(ptr_meta.ptr) + work_item_type.sizeof, _vptr_type().sizeof
            )
            q = (
                gdb.Value(q_ptr)
                .reinterpret_cast(smp_mmessage_queue_ptr_type.pointer())
                .dereference()
            )
            shard = int(q["_pending"]["remote"]["_id"])
            self._maybe_log(
                "Deduced shard is {} (message queue 0x{:x} @ 0x{:x} + {})\n".format(
                    shard, int(q), int(ptr_meta.ptr), q_ptr - int(ptr_meta.ptr)
                ),
                verbose,
            )
            # Sanity check.
            if shard < 0 or shard >= cpus():
                return None
            switch_to_shard(shard)
        else:
            print(f">>> META: {ptr_meta}")
            ptr_meta.thread.switch()

        try:
            for maybe_tptr_meta, _ in redpanda_find.find(ptr_meta.ptr):
                maybe_tptr_meta.ptr -= maybe_tptr_meta.offset_in_object
                res = self._probe_pointer(
                    maybe_tptr_meta.ptr,
                    scanned_region_size,
                    using_seastar_allocator,
                    verbose,
                )
                if res is None:
                    continue

                if int(res[0].ptr) == int(ptr_meta.ptr):
                    self._maybe_log("Rejecting self reference\n", verbose)
                    continue

                return res
        finally:
            orig.switch()

        return res

    def _walk(
        self,
        walk_method,
        tptr_meta,
        name,
        max_depth,
        scanned_region_size,
        using_seastar_allocator,
        verbose,
    ):
        i = 0
        fiber = []
        known_tasks = set([tptr_meta.ptr])
        while True:
            if max_depth > -1 and i >= max_depth:
                break

            self._maybe_log(
                "_walk() 0x{:x} {}\n".format(int(tptr_meta.ptr), name), verbose
            )
            print(">>> walk_method: {}".format(walk_method.__name__))
            res = walk_method(
                tptr_meta,
                name,
                i + 1,
                max_depth,
                scanned_region_size,
                using_seastar_allocator,
                verbose,
            )
            if res is None:
                break

            tptr_meta, vptr, name = res

            if tptr_meta.ptr in known_tasks:
                gdb.write(
                    "Stopping because loop is detected: task 0x{:016x} was seen before.\n".format(
                        tptr_meta.ptr
                    )
                )
                break

            known_tasks.add(tptr_meta.ptr)

            fiber.append((tptr_meta, vptr, name))

            i += 1

        return fiber

    def invoke(self, arg, for_tty):
        parser = argparse.ArgumentParser(description="redpanda fiber")
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            default=False,
            help="Make the command more verbose about what it is doing",
        )
        parser.add_argument(
            "-d",
            "--max-depth",
            action="store",
            type=int,
            default=-1,
            help="Maximum depth to traverse on the continuation chain",
        )
        parser.add_argument(
            "-s",
            "--scanned-region-size",
            action="store",
            type=int,
            default=512,
            help="The size of the memory region to be scanned when examining a task object."
            " Only used in fallback-mode. Fallback mode is used either when the default allocator is used by the application"
            " (and hence pointer-metadata is not available) or when `redpanda fiber` was invoked with `--force-fallback-mode`.",
        )
        parser.add_argument(
            "--force-fallback-mode",
            action="store_true",
            default=False,
            help="Force fallback mode to be used, that is, scan a fixed-size region of memory"
            " (configurable via --scanned-region-size), instead of relying on `redpanda ptr` for determining the size of the task objects.",
        )
        parser.add_argument(
            "task",
            action="store",
            help="An expression that evaluates to a valid `seastar::task*` value. Cannot contain white-space.",
        )

        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        if self._thread_map is None:
            self._thread_map = {}
            for r in reactors():
                self._thread_map[gdb.selected_thread().num] = int(r["_id"])

        def format_task_line(i, task_info):
            tptr_meta, vptr, name = task_info
            tptr = tptr_meta.ptr
            shard = self._thread_map[tptr_meta.thread.num]
            gdb.write(
                "[shard {:2}] #{:<2d} (task*) 0x{:016x} 0x{:016x} {}\n".format(
                    shard, i, int(tptr), int(vptr), name
                )
            )

        try:
            using_seastar_allocator = (
                not args.force_fallback_mode
                and redpanda_ptr.is_seastar_allocator_used()
            )
            if not using_seastar_allocator:
                gdb.write(
                    "Not using the seastar allocator, falling back to scanning a fixed-size region of memory\n"
                )

            initial_task_ptr = int(gdb.parse_and_eval(args.task))
            this_task = self._probe_pointer(
                initial_task_ptr,
                args.scanned_region_size,
                using_seastar_allocator,
                args.verbose,
            )
            if this_task is None:
                gdb.write(
                    "Provided pointer 0x{:016x} is not an object managed by seastar or not a task pointer\n".format(
                        initial_task_ptr
                    )
                )
                return

            backwards_fiber = self._walk(
                self._walk_backward,
                this_task[0],
                this_task[2],
                args.max_depth,
                args.scanned_region_size,
                using_seastar_allocator,
                args.verbose,
            )

            for i, task_info in enumerate(reversed(backwards_fiber)):
                format_task_line(i - len(backwards_fiber), task_info)

            format_task_line(0, this_task)

            forward_fiber = self._walk(
                self._walk_forward,
                this_task[0],
                this_task[2],
                args.max_depth,
                args.scanned_region_size,
                using_seastar_allocator,
                args.verbose,
            )

            for i, task_info in enumerate(forward_fiber):
                format_task_line(i + 1, task_info)

            gdb.write("\nFound no further pointers to task objects.\n")
            if not backwards_fiber and not forward_fiber:
                gdb.write(
                    "If this is unexpected, run `redpanda fiber 0x{:016x} --verbose` to learn more.\n".format(
                        initial_task_ptr
                    )
                )
            else:
                gdb.write(
                    "If you think there should be more, run `redpanda fiber 0x{:016x} --verbose` to learn more.\n"
                    "Note that continuation across user-created seastar::promise<> objects are not detected by redpanda-fiber.\n".format(
                        int(this_task[0].ptr)
                    )
                )
        except KeyboardInterrupt:
            return


class redpanda_reactors(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(
            self, "redpanda reactors", gdb.COMMAND_USER, gdb.COMPLETE_COMMAND
        )

    def print(self):
        ref = gdb.parse_and_eval("seastar::local_engine")
        reactor_backend = std_unique_ptr(ref["_backend"]).get()
        self.reactor_backend_aio = reactor_backend.dynamic_cast(
            gdb.lookup_type("seastar::reactor_backend_aio").pointer()
        ).dereference()
        self.aio_polling_io = self.reactor_backend_aio["_polling_io"]
        print(
            f"reactor_backend_aio(reactor={self.reactor_backend_aio}, polling_io={self.aio_polling_io})"
        )

    def invoke(self, arg, from_tty):
        self.print()


redpanda()
redpanda_memory()
redpanda_storage()
redpanda_batch_cache()
redpanda_task_queues()
redpanda_smp_queues()
redpanda_small_objects()
redpanda_task_histogram()
redpanda_tasks()
redpanda_heapprof()
redpanda_partitions()
redpanda_cloud_clients()
redpanda_ptr()
redpanda_find()
redpanda_fiber()
redpanda_reactors()
