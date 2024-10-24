#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#
#   - Copyright (C) 2015 ScyllaDB
#   - Copyright 2020 Redpanda Data, Inc. - libc++ support and redpanda types
#
# -----
#
# (gdb) source redpanda-gdb.py
# (gdb) redpanda memory
#
import io
import gdb
import gdb.printing
import uuid
import argparse
import re
from operator import attrgetter
from collections import defaultdict, Counter
import sys
import struct
import random
import bisect
import os
import subprocess
import time
import socket

from enum import Enum
import struct
import collections
from io import BufferedReader, BytesIO

SERDE_ENVELOPE_FORMAT = "<BBI"
SERDE_ENVELOPE_SIZE = struct.calcsize(SERDE_ENVELOPE_FORMAT)

SerdeEnvelope = collections.namedtuple('SerdeEnvelope',
                                       ('version', 'compat_version', 'size'))


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
                result |= ((i & 0x7f) << shift)
            else:
                result |= i << shift
                break
            shift += 7

        return Reader._decode_zig_zag(result)

    def with_endianness(self, str):
        ch = '<' if self.endianness == Endianness.LITTLE_ENDIAN else '>'
        return f"{ch}{str}"

    def read_int8(self):
        return struct.unpack(self.with_endianness('b'), self.stream.read(1))[0]

    def read_uint8(self):
        return struct.unpack(self.with_endianness('B'), self.stream.read(1))[0]

    def read_int16(self):
        return struct.unpack(self.with_endianness('h'), self.stream.read(2))[0]

    def read_uint16(self):
        return struct.unpack(self.with_endianness('H'), self.stream.read(2))[0]

    def read_int32(self):
        return struct.unpack(self.with_endianness('i'), self.stream.read(4))[0]

    def read_uint32(self):
        return struct.unpack(self.with_endianness('I'), self.stream.read(4))[0]

    def read_int64(self):
        return struct.unpack(self.with_endianness('q'), self.stream.read(8))[0]

    def read_uint64(self):
        return struct.unpack(self.with_endianness('Q'), self.stream.read(8))[0]

    def read_serde_enum(self):
        return self.read_int32()

    def read_iobuf(self):
        len = self.read_int32()
        return self.stream.read(len)

    def read_bool(self):
        return self.read_int8() == 1

    def read_string(self):
        len = self.read_int32()
        return self.stream.read(len).decode('utf-8')

    def read_kafka_string(self):
        len = self.read_int16()
        return self.stream.read(len).decode('utf-8')

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
        return self.stream.read(len).decode('utf-8')

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
                return {
                    'envelope': envelope
                } | type_read(self, envelope.version)
            else:
                return {
                    'error': {
                        'max_supported_version': max_version,
                        'envelope': envelope
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
            t['state'] = 'disabled'
        elif state == 0:
            t['state'] = 'empty'
        else:
            t['value'] = type_read(self)
        return t

    def read_bytes(self, length):
        return self.stream.read(length)

    def peek(self, length):
        return self.stream.peek(length)

    def read_uuid(self):
        return ''.join([
            f'{self.read_uint8():02x}' + ('-' if k in [3, 5, 7, 9] else '')
            for k in range(16)
        ])

    def peek_int8(self):
        # peek returns the whole memory buffer, slice is needed to conform to struct format string
        return struct.unpack('<b', self.stream.peek(1)[:1])[0]

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
        # return self.obj['_M_t']['_M_t']['_M_head_impl']
        return self.obj['__ptr_']['__value_']

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
            return self.ref['__val_']
        except gdb.error:
            return self.ref['__value_'].dereference()

    def __bool__(self):
        try:
            return bool(self.ref['__engaged_'])
        except gdb.error:
            return bool(self.ref['__value_'])


class fragmented_vector:
    def __init__(self, ref):
        self.ref = ref

        self.container_type = self.ref.type.strip_typedefs()
        self.element_type = self.container_type.template_argument(0)
        self.element_size_bytes = self.element_type.sizeof

    def size_bytes_capacity(self):
        return self.capacity() * self.element_size_bytes

    def __len__(self):
        return int(self.ref['_size'])

    def capacity(self):
        return int(self.ref['_capacity'])

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
            self.end_cap_type = gdb.lookup_type(
                end_cap_type_fmt.format(end_cap_type))
        except:
            # Try converting "struct foo *" into "foo*": sometimes GDB reports the type
            # one way, but expects us to give it the other way
            # For example:
            #   std::__1::__compressed_pair_elem<seastar::shared_object*, 0, false>)
            s = str(end_cap_type)
            m = re.match("struct ([\w:]+) \*", s)
            if m:
                self.end_cap_type = gdb.lookup_type(
                    end_cap_type_fmt.format(m.group(1) + "*"))
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
        return int(self.ref['__end_'] - self.ref['__begin_'])

    def __iter__(self):
        i = self.ref['__begin_']
        end = self.ref['__end_']
        while i != end:
            yield i.dereference()
            i += 1

    def __getitem__(self, item):
        return (self.ref['__begin_'] + item).dereference()

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
            self.offset(type_idx - 1) +
            self.types[type_idx - 1].sizeof * self.sizes[type_idx - 1],
            self.types[type_idx].alignof)

    def pointer(self, type, base_ptr):

        ptr = base_ptr + self.offset(self.element_index(type))
        ptr_type = type.pointer().strip_typedefs()
        ptr_v = gdb.parse_and_eval(f"({ptr_type}){ptr}")
        return ptr_v


class absl_btree_map_node:
    def __init__(self, ref: gdb.Value):
        self.type = ref.type.strip_typedefs()
        self.ref = ref
        self.layout_type = gdb.lookup_type(
            f"{self.type}::layout_type").strip_typedefs()
        self.slots = gdb.parse_and_eval(f"(int){self.type}::kNodeSlots")
        self.params = absl_btree_map_params(self.type.template_argument(0))
        self.internal_layout = absl_layout(self.layout_type, 1, 0, 4,
                                           self.slots, self.slots + 1)
        self.leaf_layout = absl_layout(self.layout_type, 1, 0, 4, self.slots,
                                       0)

    def get_field(self, idx):
        tp = self.internal_layout.types[idx]

        return self.internal_layout.pointer(tp, self.ref.address)

    def parent(self):
        return absl_btree_map_node(
            self.get_field(0).dereference().dereference())

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
        return absl_btree_map_node(
            self.get_field(4)[idx].dereference().dereference())

    def is_internal(self):
        return not self.is_leaf()

    def start_child(self):
        return self.child(self.start())


class absl_btree_map:
    def __init__(self, ref):
        self.ref = ref
        container_type = self.ref.type.strip_typedefs()
        self.tree = ref['tree_']
        self.tree_type = self.tree.type
        self.kt = container_type.template_argument(0)
        self.vt = container_type.template_argument(1)

        self.root = absl_btree_map_node(self.tree['root_'].dereference())
        self.rightmost_node = absl_btree_map_node(
            self.tree['rightmost_']['value'].dereference())

        # iterator part
        self.node_it = self.leftmost()
        self.pos_it = self.leftmost().start()

    def leftmost(self):
        return self.root.parent()

    def __iter__(self):

        while True:

            value = self.node_it.slot(self.pos_it)['value']
            yield value["first"], value["second"]

            # node_->is_leaf() && ++position_ < node_->finish()
            self.pos_it += 1
            if self.node_it.ref.address == self.rightmost_node.ref.address and self.pos_it == self.node_it.finish(
            ):
                break

            if self.node_it.is_leaf() and self.pos_it < self.node_it.finish():
                # already incremented position iterator
                continue
            # increment_slow
            if self.node_it.is_leaf():
                while self.pos_it == self.node_it.finish(
                ) and not self.node_it.is_root():
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


class absl_flat_hash_map:
    signed_byte_t = gdb.lookup_type('int8_t')

    def __init__(self, p):
        self.map = p
        container_type = self.map.type.strip_typedefs()
        self.kt = container_type.template_argument(0)
        self.vt = container_type.template_argument(1)
        self._begin()

    def capacity(self):
        return self.map["capacity_"]

    def __len__(self):
        return self.map["size_"]

    def _begin(self):
        self.it_ctrl = self.map["ctrl_"]
        self.it_slot = self.map["slots_"]
        self._skip_empty_or_deleted()

    def __iter__(self):
        while self.it_ctrl != (self.map["ctrl_"] + self.map["capacity_"]):
            value = self.it_slot["value"]
            yield value["first"], value["second"]
            self.it_ctrl += 1
            self.it_slot += 1
            self._skip_empty_or_deleted()

    def _skip_empty_or_deleted(self):
        while self._ctrl_value() < -1:
            self.it_ctrl += 1
            self.it_slot += 1

    def _ctrl_value(self):
        return self.it_ctrl.cast(self.signed_byte_t.pointer()).dereference()


def has_enable_lw_shared_from_this(type):
    for f in type.fields():
        if f.is_base_class and 'enable_lw_shared_from_this' in f.name:
            return True
    return False


def remove_prefix(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


class seastar_lw_shared_ptr():
    def __init__(self, ref):
        self.ref = ref
        self.elem_type = ref.type.template_argument(0)

    def _no_esft_type(self):
        try:
            return gdb.lookup_type(
                'seastar::lw_shared_ptr_no_esft<%s>' % remove_prefix(
                    str(self.elem_type.unqualified()), 'class ')).pointer()
        except:
            return gdb.lookup_type(
                'seastar::shared_ptr_no_esft<%s>' % remove_prefix(
                    str(self.elem_type.unqualified()), 'class ')).pointer()

    def get(self):
        if has_enable_lw_shared_from_this(self.elem_type):
            return self.ref['_p'].cast(self.elem_type.pointer())
        else:
            return self.ref['_p'].cast(self._no_esft_type())['_value'].address


class seastar_shared_ptr():
    def __init__(self, ref):
        self.ref = ref

    def get(self):
        return self.ref['_p']


class seastar_sstring:
    def __init__(self, ref):
        self.ref = ref

    def __len__(self):
        if self.ref['u']['internal']['size'] >= 0:
            return int(self.ref['u']['internal']['size'])
        else:
            return int(self.ref['u']['external']['size'])


class seastar_circular_buffer(object):
    def __init__(self, ref):
        self.ref = ref

    def _mask(self, i):
        return i & (int(self.ref['_impl']['capacity']) - 1)

    def __iter__(self):
        impl = self.ref['_impl']
        st = impl['storage']
        cap = impl['capacity']
        i = impl['begin']
        end = impl['end']
        while i < end:
            yield st[self._mask(i)]
            i += 1

    def size(self):
        impl = self.ref['_impl']
        return int(impl['end']) - int(impl['begin'])

    def __len__(self):
        return self.size()

    def __getitem__(self, item):
        impl = self.ref['_impl']
        return (impl['storage'] +
                self._mask(int(impl['begin']) + item)).dereference()

    def external_memory_footprint(self):
        impl = self.ref['_impl']
        return int(
            impl['capacity']) * self.ref.type.template_argument(0).sizeof


class seastar_static_vector:
    def __init__(self, ref):
        self.ref = ref

    def __len__(self):
        return int(self.ref['m_holder']['m_size'])

    def __iter__(self):
        t = self.ref.type.strip_typedefs()
        value_type = t.template_argument(0)
        try:
            data = self.ref['m_holder']['storage']['data'].cast(
                value_type.pointer())
        except:
            try:
                data = self.ref['m_holder']['storage']['dummy']['dummy'].cast(
                    value_type.pointer())  # Scylla 3.1 compatibility
            except gdb.error:
                data = self.ref['m_holder']['storage']['dummy'].cast(
                    value_type.pointer())  # Scylla 3.0 compatibility
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
            return ''

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
                indicator = '+' * max(1, int(count * count_per_column))
            else:
                indicator = ''
            for item in items:
                lines.append('{:9d} {} {}'.format(count, self._formatter(item),
                                                  indicator))

        return '\n'.join(lines)

    def __repr__(self):
        return 'histogram({})'.format(self._counts)

    def print_to_console(self):
        gdb.write(str(self) + '\n')


def cpus():
    return int(gdb.parse_and_eval('::seastar::smp::count'))


def current_shard():
    return int(gdb.parse_and_eval('\'seastar\'::local_engine->_id'))


def get_local_task_queues():
    """ Return a list of task pointers for the local reactor. """
    for tq_ptr in seastar_static_vector(
            gdb.parse_and_eval('\'seastar\'::local_engine._task_queues')):
        yield std_unique_ptr(tq_ptr).dereference()


# addr (int) -> name (str)
names = {}


def resolve(addr, cache=True, startswith=None):
    if addr in names:
        return names[addr]

    infosym = gdb.execute('info symbol 0x%x' % (addr), False, True)
    if infosym.startswith('No symbol'):
        return None

    name = infosym[:infosym.find('in section')]
    if startswith and not name.startswith(startswith):
        return None
    if cache:
        names[addr] = name
    return name


def get_text_range():
    try:
        vptr_type = gdb.lookup_type('uintptr_t').pointer()
        reactor_backend = gdb.parse_and_eval('seastar::local_engine->_backend')
        reactor_backend = std_unique_ptr(reactor_backend).get()
        # NOAH in clang it looks like things start with std::__1::unique_ptr
        ## 2019.1 has value member, >=3.0 has std::unique_ptr<>
        #if reactor_backend.type.strip_typedefs().name.startswith('std::unique_ptr<'):
        #    reactor_backend = std_unique_ptr(reactor_backend).get()
        #else:
        #    reactor_backend = gdb.parse_and_eval('&seastar::local_engine->_backend')
        known_vptr = int(
            reactor_backend.reinterpret_cast(vptr_type).dereference())
    except Exception as e:
        gdb.write(
            "get_text_range(): Falling back to locating .rodata section because lookup to reactor backend to use as known vptr failed: {}\n"
            .format(e))
        known_vptr = None

    sections = gdb.execute('info files', False, True).split('\n')
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
    cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
    page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
    mem_start = cpu_mem['memory']
    vptr_type = gdb.lookup_type('uintptr_t').pointer()
    pages = cpu_mem['pages']
    nr_pages = int(cpu_mem['nr_pages'])

    text_start, text_end = get_text_range()

    def is_vptr(addr):
        return addr >= text_start and addr <= text_end

    idx = 0
    while idx < nr_pages:
        if pages[idx]['free']:
            idx += pages[idx]['span_size']
            continue
        pool = pages[idx]['pool']
        if not pool or pages[idx]['offset_in_span'] != 0:
            idx += 1
            continue
        objsize = int(pool.dereference()['_object_size'])
        span_size = pages[idx]['span_size'] * page_size
        for idx2 in range(0, int(span_size / objsize) + 1):
            obj_addr = mem_start + idx * page_size + idx2 * objsize
            vptr = obj_addr.reinterpret_cast(vptr_type).dereference()
            if is_vptr(vptr):
                yield obj_addr, vptr
        idx += pages[idx]['span_size']


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
        return self.page['free']

    def pool(self):
        """
        Returns seastar::memory::small_pool* of this span.
        Valid only when is_small().
        """
        return self.page['pool']

    def is_small(self):
        return not self.is_free() and self.page['pool']

    def is_large(self):
        return not self.is_free() and not self.page['pool']

    def size(self):
        return int(self.page['span_size'])

    def used_span_size(self):
        """
        Returns the number of pages at the front of the span which are used by the allocator.

        Due to https://github.com/scylladb/seastar/issues/625 there may be some
        pages at the end of the span which are not used by the small pool.
        We try to detect this. It's not 100% accurate but should work in most cases.

        Returns 0 for free spans.
        """
        n_pages = 0
        pool = self.page['pool']
        if self.page['free']:
            return 0
        if not pool:
            return self.page['span_size']
        for idx in range(int(self.page['span_size'])):
            page = self.page.address + idx
            if not page['pool'] or page['pool'] != pool or page[
                    'offset_in_span'] != idx:
                break
            n_pages += 1
        return n_pages


def spans():
    cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
    page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
    nr_pages = int(cpu_mem['nr_pages'])
    pages = cpu_mem['pages']
    mem_start = int(cpu_mem['memory'])
    idx = 1
    while idx < nr_pages:
        page = pages[idx]
        span_size = int(page['span_size'])
        if span_size == 0:
            idx += 1
            continue
        last_page = pages[idx + span_size - 1]
        addr = mem_start + idx * page_size
        yield span(idx, addr, page)
        idx += span_size


class span_checker(object):
    def __init__(self):
        self._page_size = int(
            gdb.parse_and_eval('\'seastar::memory::page_size\''))
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
        if span_start + s.page['span_size'] * self._page_size <= ptr:
            return None
        return s


def find_storage_api(shard=None):
    if shard is None:
        shard = current_shard()
    return gdb.parse_and_eval('debug::app')['storage']['_instances'][
        '__begin_'][shard]['service']['_p']


class index_state:
    def __init__(self, ref):
        self.ref = ref
        self.offset = fragmented_vector(self.ref["relative_offset_index"])
        self.time = fragmented_vector(self.ref["relative_time_index"])
        self.pos = fragmented_vector(self.ref["position_index"])

    def size(self):
        return int(self.offset.size_bytes() + self.time.size_bytes() +
                   self.pos.size_bytes())

    def capacities(self):
        return (int(x) for x in (self.offset.size_bytes_capacity(),
                                 self.time.size_bytes_capacity(),
                                 self.pos.size_bytes_capacity()))

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
        self.filename = self.ref["_filename"]

    def __str__(self):
        return "{}".format(self.filename)


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
        return str(self.ref['_value'])


class offset_tracker:
    def __init__(self, ref):
        self.ref = ref

    @property
    def base_offset(self):
        return model_offset(self.ref['base_offset'])

    @property
    def dirty_offset(self):
        return model_offset(self.ref['dirty_offset'])

    @property
    def term(self):
        return model_offset(self.ref['term'])

    @property
    def committed_offset(self):
        return model_offset(self.ref['committed_offset'])

    @property
    def stable_offset(self):
        return model_offset(self.ref['stable_offset'])

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
        return offset_tracker(self.ref['_tracker'])

    def reader(self):
        return segment_reader(self.ref["_reader"])

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
        return self.ref['ns']['_value']

    def topic(self):
        return self.ref['tp']['topic']['_value']

    def partition(self):
        return self.ref['tp']['partition']['_value']


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
        if field.is_base_class and re.match(name_pattern,
                                            field.type.strip_typedefs().name):
            return int(field.bitpos / 8)


class boost_intrusive_list:
    size_t = gdb.lookup_type('size_t')

    def __init__(self, list_ref, link=None):
        list_type = list_ref.type.strip_typedefs()
        self.node_type = list_type.template_argument(0)
        rps = list_ref['data_']['root_plus_size_']
        try:
            self.root = rps['root_']
        except gdb.error:
            # Some boost versions have this instead
            self.root = rps['m_header']
        if link is not None:
            self.link_offset = get_field_offset(self.node_type, link)
        else:
            member_hook = get_template_arg_with_prefix(
                list_type, "boost::intrusive::member_hook")

            if not member_hook:
                member_hook = get_template_arg_with_prefix(
                    list_type, "struct boost::intrusive::member_hook")
            if member_hook:
                self.link_offset = member_hook.template_argument(2).cast(
                    self.size_t)
            else:
                self.link_offset = get_base_class_offset(
                    self.node_type, "boost::intrusive::list_base_hook")
                if self.link_offset is None:
                    raise Exception("Class does not extend list_base_hook: " +
                                    str(self.node_type))

    def __iter__(self):
        hook = self.root['next_']
        while hook and hook != self.root.address:
            node_ptr = hook.cast(self.size_t) - self.link_offset
            yield node_ptr.cast(self.node_type.pointer()).dereference()
            hook = hook['next_']

    def __nonzero__(self):
        return self.root['next_'] != self.root.address

    def __bool__(self):
        return self.__nonzero__()

    def __len__(self):
        return len(list(iter(self)))


class readers_cache:
    def __init__(self, ref):
        self.ref = ref
        self.readers = boost_intrusive_list(self.ref['_readers'], "_hook")
        self.in_use = boost_intrusive_list(self.ref['_in_use'], "_hook")


class disk_log_impl:
    disk_log_impl_t = gdb.lookup_type("storage::disk_log_impl")

    def __init__(self, ref):
        self.ref = ref.cast(self.disk_log_impl_t.pointer())

    def segments(self):
        return segment_set(self.ref["_segs"])

    def readers_cache(self):
        return readers_cache(std_unique_ptr(self.ref["_readers_cache"]).get())


def find_logs(shard=None):
    storage = find_storage_api(shard)
    log_mgr = std_unique_ptr(storage["_log_mgr"]).dereference()
    for ntp, log in absl_flat_hash_map(log_mgr["_logs"]):
        log_meta_ptr = std_unique_ptr(log)
        impl = seastar_shared_ptr(log_meta_ptr["handle"]["_impl"]).get()
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
        gdb.Command.__init__(self, 'redpanda memory', gdb.COMMAND_USER,
                             gdb.COMPLETE_COMMAND)

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
        print(f"Total capacity: {sum(capacities)//1024} KB")
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

        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
        free_mem = int(cpu_mem['nr_free_pages']) * page_size
        total_mem = int(cpu_mem['nr_pages']) * page_size
        gdb.write(
            'Used memory: {used_mem:>13}\nFree memory: {free_mem:>13}\nTotal memory: {total_mem:>12}\n\n'
            .format(used_mem=total_mem - free_mem,
                    free_mem=free_mem,
                    total_mem=total_mem))

        gdb.write('Small pools:\n')
        small_pools = cpu_mem['small_pools']
        nr = small_pools['nr_small_pools']
        gdb.write(
            '{objsize:>5} {span_size:>6} {use_count:>10} {memory:>12} {unused:>12} {wasted_percent:>5}\n'
            .format(objsize='objsz',
                    span_size='spansz',
                    use_count='usedobj',
                    memory='memory',
                    unused='unused',
                    wasted_percent='wst%'))
        total_small_bytes = 0
        sc = span_checker()
        for i in range(int(nr)):
            sp = small_pools['_u']['a'][i]
            object_size = int(sp['_object_size'])
            span_size = int(sp['_span_sizes']['preferred']) * page_size
            free_count = int(sp['_free_count'])
            pages_in_use = 0
            use_count = 0
            for s in sc.spans():
                if s.pool() == sp.address:
                    pages_in_use += s.size()
                    use_count += int(s.used_span_size() * page_size /
                                     object_size)
            memory = pages_in_use * page_size
            total_small_bytes += memory
            use_count -= free_count
            wasted = free_count * object_size
            unused = memory - use_count * object_size
            wasted_percent = wasted * 100.0 / memory if memory else 0
            gdb.write(
                '{objsize:5} {span_size:6} {use_count:10} {memory:12} {unused:12} {wasted_percent:5.1f}\n'
                .format(objsize=object_size,
                        span_size=span_size,
                        use_count=use_count,
                        memory=memory,
                        unused=unused,
                        wasted_percent=wasted_percent))
        gdb.write('Small allocations: %d [B]\n' % total_small_bytes)

        large_allocs = defaultdict(
            int)  # key: span size [B], value: span count
        for s in sc.spans():
            span_size = s.size()
            if s.is_large():
                large_allocs[span_size * page_size] += 1

        gdb.write('Page spans:\n')
        gdb.write(
            '{index:5} {size:>13} {total:>13} {allocated_size:>13} {allocated_count:>7}\n'
            .format(index="index",
                    size="size [B]",
                    total="free [B]",
                    allocated_size="large [B]",
                    allocated_count="[spans]"))
        total_large_bytes = 0
        for index in range(int(cpu_mem['nr_span_lists'])):
            span_list = cpu_mem['free_spans'][index]
            front = int(span_list['_front'])
            pages = cpu_mem['pages']
            total = 0
            while front:
                span = pages[front]
                total += int(span['span_size'])
                front = int(span['link']['_next'])
            span_size = (1 << index) * page_size
            allocated_size = large_allocs[span_size] * span_size
            total_large_bytes += allocated_size
            gdb.write(
                '{index:5} {size:13} {total:13} {allocated_size:13} {allocated_count:7}\n'
                .format(index=index,
                        size=span_size,
                        total=total * page_size,
                        allocated_count=large_allocs[span_size],
                        allocated_size=allocated_size))
        gdb.write('Large allocations: %d [B]\n' % total_large_bytes)


class redpanda_storage(gdb.Command):
    """Summarize the state of redpanda storage layer
    """
    def __init__(self):
        gdb.Command.__init__(self, 'redpanda storage', gdb.COMMAND_USER,
                             gdb.COMPLETE_COMMAND)

    def print_segments(self):
        print(f"# Log segments")

        for ntp, log in find_logs():
            print(f"{ntp} segment count {log.segments().size()}")
            for segment in log.segments():
                offsets = segment.offsets_tracker()
                print(f"{ntp} - {offsets}")

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


class iobuf:
    def __init__(self, ref):
        self.size = ref['_size']
        self.fragments = boost_intrusive_list(ref['_frags'], "hook")

    def __str__(self):
        return f"{{ size: {self.size} }}"


def iobuf_bytes(buf):
    bytes = io.BytesIO()
    for f in buf.fragments:
        used_bytes = f['_used_bytes']
        buffer = f['_buf']
        for i in range(used_bytes):
            bytes.write(
                int(buffer['_buffer'][i].format_string(format='u')).to_bytes(
                    1, byteorder='little'))
    bytes.seek(0)
    return bytes


class batch_cache_range:
    def __init__(self, ref):
        self.ref = ref
        self.valid = ref['_valid']
        self.arena = iobuf(ref['_arena'])
        self.offsets = std_vector(ref['_offsets'])
        self.pinned = ref['_pinned']
        self.size = ref['_size']

    def __str__(self):
        return f"{{ address: {self.ref.address}, size: {self.size}, pinned: {self.pinned}, valid: {self.valid}, offsets: [{','.join([ str(o['_value']) for o in self.offsets])}] }}"


class batch_cache_entry:
    def __init__(self, ref):
        self.ref = ref
        self.range_offset = ref['_range_offset']
        self.range = batch_cache_range(ref['_range']['_ptr'].dereference())

    def header(self):
        bytes = iobuf_bytes(self.range.arena)
        # seek to given offset
        bytes.seek(self.range_offset)
        reader = Reader(bytes)
        return {
            'header_crc': reader.read_uint32(),
            'size': reader.read_int32(),
            'base_offset': reader.read_int64(),
            'type': reader.read_int8(),
            'crc': reader.read_int32(),
            'attrs': reader.read_int16(),
            'last_offset_delta': reader.read_int32(),
            'first_ts': reader.read_int64(),
            'max_ts': reader.read_int64(),
            'producer_id': reader.read_int64(),
            'producer_epoch': reader.read_int16(),
            'base_sequence': reader.read_int32(),
            'record_count': reader.read_int32(),
            'term': reader.read_int64(),
        }


class redpanda_batch_cache(gdb.Command):
    """Prints content of redpanda batch cache
    """
    def __init__(self):
        gdb.Command.__init__(self, 'redpanda batch_cache', gdb.COMMAND_USER,
                             gdb.COMPLETE_COMMAND)

    def get_log(self, ns, topic, partition):
        for ntp, log in find_logs():
            m_ntp = model_ntp(ntp)
            if str(m_ntp.namespace()).strip('"') == ns and str(
                    m_ntp.topic()).strip('"') == topic and partition == str(
                        m_ntp.partition()):
                return log
        return None

    def invoke(self, arg, from_tty):
        ns, tp, partition = arg.split('/')
        print(f"# Batch cache for {ns}/{tp}/{partition}")
        log = self.get_log(ns, tp, partition)
        for s in log.segments():

            for k, v in s.batch_cache_index():
                range_offset = k['_value']
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

    For usage see: scylla small-objects --help

    Examples:

    (gdb) scylla small-objects -o 32 --summarize
    number of objects: 60196912
    page size        : 20
    number of pages  : 3009845

    (gdb) scylla small-objects -o 32 -p 100
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
    class small_object_iterator():
        def __init__(self, small_pool, resolve_symbols):
            self._small_pool = small_pool
            self._resolve_symbols = resolve_symbols

            self._text_start, self._text_end = get_text_range()
            self._vptr_type = gdb.lookup_type('uintptr_t').pointer()
            self._free_object_ptr = gdb.lookup_type('void').pointer().pointer()
            self._page_size = int(
                gdb.parse_and_eval('\'seastar::memory::page_size\''))
            self._free_in_pool = set()
            self._free_in_span = set()

            pool_next_free = self._small_pool['_free']
            while pool_next_free:
                self._free_in_pool.add(int(pool_next_free))
                pool_next_free = pool_next_free.reinterpret_cast(
                    self._free_object_ptr).dereference()

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
            span_next_free = span.page['freelist']
            while span_next_free:
                self._free_in_span.add(int(span_next_free))
                span_next_free = span_next_free.reinterpret_cast(
                    self._free_object_ptr).dereference()

            return span_start, span_end

        def _next_obj(self):
            try:
                return next(self._obj_it)
            except StopIteration:
                # Don't call self._next_span() here as it might throw another StopIteration.
                pass

            span_start, span_end = self._next_span()
            self._obj_it = iter(
                range(span_start, span_end,
                      int(self._small_pool['_object_size'])))
            return next(self._obj_it)

        def __next__(self):
            obj = self._next_obj()
            while obj in self._free_in_span or obj in self._free_in_pool:
                obj = self._next_obj()

            if self._resolve_symbols:
                addr = gdb.Value(obj).reinterpret_cast(
                    self._vptr_type).dereference()
                if addr >= self._text_start and addr <= self._text_end:
                    return (obj, resolve(addr))
                else:
                    return (obj, None)
            else:
                return (obj, None)

        def __iter__(self):
            return self

    def __init__(self):
        gdb.Command.__init__(self, 'redpanda small-objects', gdb.COMMAND_USER,
                             gdb.COMPLETE_COMMAND)

        self._parser = None
        self._iterator = None
        self._last_pos = 0
        self._last_object_size = None

    @staticmethod
    def get_object_sizes():
        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        small_pools = cpu_mem['small_pools']
        nr = int(small_pools['nr_small_pools'])
        return [
            int(small_pools['_u']['a'][i]['_object_size']) for i in range(nr)
        ]

    @staticmethod
    def find_small_pool(object_size):
        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        small_pools = cpu_mem['small_pools']
        nr = int(small_pools['nr_small_pools'])
        for i in range(nr):
            sp = small_pools['_u']['a'][i]
            if object_size == int(sp['_object_size']):
                return sp

        return None

    def init_parser(self):
        parser = argparse.ArgumentParser(description="scylla small-objects")
        parser.add_argument("-o",
                            "--object-size",
                            action="store",
                            type=int,
                            required=True,
                            help="Object size, valid sizes are: {}".format(
                                redpanda_small_objects.get_object_sizes()))
        parser.add_argument("-p",
                            "--page",
                            action="store",
                            type=int,
                            default=0,
                            help="Page to show.")
        parser.add_argument(
            "-s",
            "--page-size",
            action="store",
            type=int,
            default=20,
            help=
            "Number of objects in a page. A page size of 0 turns off paging.")
        parser.add_argument("--random-page",
                            action="store_true",
                            help="Show a random page.")
        parser.add_argument(
            "--summarize",
            action="store_true",
            help="Print the number of objects and pages in the pool.")
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Print additional details on what is going on.")

        self._parser = parser

    def get_objects(self,
                    small_pool,
                    offset=0,
                    count=0,
                    resolve_symbols=False,
                    verbose=False):
        if self._last_object_size != int(
                small_pool['_object_size']) or offset < self._last_pos:
            self._last_pos = 0
            self._iterator = redpanda_small_objects.small_object_iterator(
                small_pool, resolve_symbols)

        skip = offset - self._last_pos
        if verbose:
            gdb.write(
                'get_objects(): offset={}, count={}, last_pos={}, skip={}\n'.
                format(offset, count, self._last_pos, skip))

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
                redpanda_small_objects.get_object_sizes())

        if args.summarize:
            if self._last_object_size != args.object_size:
                if args.verbose:
                    gdb.write(
                        "Object size changed ({} -> {}), scanning pool.\n".
                        format(self._last_object_size, args.object_size))
                self._num_objects = len(
                    self.get_objects(small_pool, verbose=args.verbose))
                self._last_object_size = args.object_size
            gdb.write("number of objects: {}\n"
                      "page size        : {}\n"
                      "number of pages  : {}\n".format(
                          self._num_objects, args.page_size,
                          int(self._num_objects / args.page_size)))
            return

        if args.random_page:
            if self._last_object_size != args.object_size:
                if args.verbose:
                    gdb.write(
                        "Object size changed ({} -> {}), scanning pool.\n".
                        format(self._last_object_size, args.object_size))
                self._num_objects = len(
                    self.get_objects(small_pool, verbose=args.verbose))
                self._last_object_size = args.object_size
            page = random.randint(0,
                                  int(self._num_objects / args.page_size) - 1)
        else:
            page = args.page

        offset = page * args.page_size
        gdb.write("page {}: {}-{}\n".format(page, offset,
                                            offset + args.page_size - 1))
        for i, (obj, sym) in enumerate(
                self.get_objects(small_pool,
                                 offset,
                                 args.page_size,
                                 resolve_symbols=True,
                                 verbose=args.verbose)):
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
        gdb.Command.__init__(self, 'redpanda task_histogram', gdb.COMMAND_USER,
                             gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        parser = argparse.ArgumentParser(description="redpanda task_histogram")
        parser.add_argument(
            "-m",
            "--samples",
            action="store",
            type=int,
            default=20000,
            help=
            "The number of samples to collect. Defaults to 20000. Set to 0 to sample all objects. Ignored when `--all` is used."
            " Note that due to this limit being checked only after scanning an entire page, in practice it will always be overshot."
        )
        parser.add_argument(
            "-c",
            "--count",
            action="store",
            type=int,
            default=30,
            help=
            "Show only the top COUNT elements of the histogram. Defaults to 30. Set to 0 to show all items. Ignored when `--all` is used."
        )
        parser.add_argument(
            "-a",
            "--all",
            action="store_true",
            default=False,
            help=
            "Sample all pages and show all results. Equivalent to -m=0 -c=0.")
        parser.add_argument(
            "-s",
            "--size",
            action="store",
            default=0,
            help=
            "The size of objects to sample. When set, only objects of this size will be sampled. A size of 0 (the default value) means no size restrictions."
        )
        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        size = args.size
        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
        mem_start = cpu_mem['memory']

        vptr_type = gdb.lookup_type('uintptr_t').pointer()

        pages = cpu_mem['pages']
        nr_pages = int(cpu_mem['nr_pages'])
        page_samples = range(0, nr_pages) if args.all else random.sample(
            range(0, nr_pages), nr_pages)

        text_start, text_end = get_text_range()

        sc = span_checker()
        vptr_count = defaultdict(int)
        scanned_pages = 0
        for idx in page_samples:
            span = sc.get_span(mem_start + idx * page_size)
            if not span or span.index != idx or not span.is_small():
                continue
            pool = span.pool()
            if int(pool.dereference()['_object_size']) != size and size != 0:
                continue
            scanned_pages += 1
            objsize = size if size != 0 else int(
                pool.dereference()['_object_size'])
            span_size = span.used_span_size() * page_size
            for idx2 in range(0, int(span_size / objsize)):
                obj_addr = span.start + idx2 * objsize
                addr = gdb.Value(obj_addr).reinterpret_cast(
                    vptr_type).dereference()
                if addr >= text_start and addr <= text_end:
                    vptr_count[int(addr)] += 1
            if args.all or args.samples == 0:
                continue
            if scanned_pages >= args.samples or len(
                    vptr_count) >= args.samples:
                break

        sorted_counts = sorted(vptr_count.items(), key=lambda e: -e[1])
        to_show = sorted_counts if args.all or args.count == 0 else sorted_counts[:args
                                                                                  .
                                                                                  count]
        for vptr, count in to_show:
            sym = resolve(vptr)
            if sym:
                gdb.write('%10d: 0x%x %s\n' % (count, vptr, sym))


class redpanda_task_queues(gdb.Command):
    """ Print a summary of the reactor's task queues.

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
        gdb.Command.__init__(self, 'redpanda task-queues', gdb.COMMAND_USER,
                             gdb.COMPLETE_NONE, True)

    @staticmethod
    def _active(a):
        if a:
            return 'A'
        return ' '

    @staticmethod
    def _current(c):
        if c:
            return '*'
        return ' '

    def invoke(self, arg, for_tty):
        gdb.write('   {:2} {:32} {:7} {}\n'.format("id", "name", "shares",
                                                   "tasks"))
        for tq in get_local_task_queues():
            gdb.write('{}{} {:02} {:32} {:>7.2f} {}\n'.format(
                self._current(bool(tq['_current'])),
                self._active(bool(tq['_active'])), int(tq['_id']),
                str(tq['_name']), float(tq['_shares']),
                len(seastar_circular_buffer(tq['_q']))))


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
        gdb.Command.__init__(self, 'redpanda smp-queues', gdb.COMMAND_USER,
                             gdb.COMPLETE_COMMAND)
        self.queues = set()

    def _init(self):
        qs = std_unique_ptr(gdb.parse_and_eval('seastar::smp::_qs')).get()
        for i in range(cpus()):
            for j in range(cpus()):
                self.queues.add(int(qs[i][j].address))
        self._queue_type = gdb.lookup_type(
            'seastar::smp_message_queue').pointer()
        self._ptr_type = gdb.lookup_type('uintptr_t').pointer()

    def invoke(self, arg, from_tty):
        if not self.queues:
            self._init()

        def formatter(q):
            a, b = q
            return '{:2} -> {:2}'.format(a, b)

        h = histogram(formatter=formatter)
        known_vptrs = dict()

        for obj, vptr in find_vptrs():
            obj = int(obj)
            vptr = int(vptr)

            if not vptr in known_vptrs:
                name = resolve(
                    vptr,
                    startswith=
                    'vtable for seastar::smp_message_queue::async_work_item')
                if name:
                    known_vptrs[vptr] = None
                else:
                    continue

            offset = known_vptrs[vptr]

            if offset is None:
                q = None
                ptr_meta = scylla_ptr.analyze(obj)
                for offset in range(0, ptr_meta.size, self._ptr_type.sizeof):
                    ptr = int(
                        gdb.Value(obj + offset).reinterpret_cast(
                            self._ptr_type).dereference())
                    if ptr in self.queues:
                        q = gdb.Value(ptr).reinterpret_cast(
                            self._queue_type).dereference()
                        break
                known_vptrs[vptr] = offset
                if q is None:
                    continue
            else:
                ptr = int(
                    gdb.Value(obj + offset).reinterpret_cast(
                        self._ptr_type).dereference())
                q = gdb.Value(ptr).reinterpret_cast(
                    self._queue_type).dereference()

            a = int(q['_completed']['remote']['_id'])
            b = int(q['_pending']['remote']['_id'])
            h[(a, b)] += 1

        gdb.write('{}\n'.format(h))


class redpanda(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'redpanda', gdb.COMMAND_USER,
                             gdb.COMPLETE_COMMAND, True)


class sstring_printer(gdb.printing.PrettyPrinter):
    'print an sstring'
    def __init__(self, val):
        self.val = val

    def to_string(self):
        if self.val['u']['internal']['size'] >= 0:
            array = self.val['u']['internal']['str']
            len = int(self.val['u']['internal']['size'])
            return ''.join([chr(array[x]) for x in range(len)])
        else:
            # TODO: looks broken for external?
            return self.val['u']['external']['str']

    def display_hint(self):
        return 'string'


class model_ntp_printer(gdb.printing.PrettyPrinter):
    'print a model::ntp'
    def __init__(self, val):
        self.val = val

    def to_string(self):
        ns = self.val['ns']['_value']
        topic = self.val['tp']['topic']['_value']
        partition = self.val['tp']['partition']['_value']
        return f"{{{ns}}}.{{{topic}}}.{{{partition}}}"

    def display_hint(self):
        return 'model::ntp'


def build_pretty_printer():
    pp = gdb.printing.RegexpCollectionPrettyPrinter('redpanda')
    pp.add_printer('sstring', r'^seastar::basic_sstring<char,.*>$',
                   sstring_printer)
    pp.add_printer('model::ntp', r'^model::ntp$', model_ntp_printer)
    return pp


gdb.printing.register_pretty_printer(gdb.current_objfile(),
                                     build_pretty_printer(),
                                     replace=True)


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
        return {'size': self.size, 'count': self.count}


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


def print_tree(root_node,
               formatter=attrgetter('key'),
               order_by=attrgetter('key'),
               printer=sys.stdout.write,
               node_filter=None):
    def print_node(node, is_last_history):
        stems = (" |   ", "     ")
        branches = (" |-- ", " \-- ")

        label_lines = formatter(node).rstrip('\n').split('\n')
        prefix_without_branch = ''.join(
            map(stems.__getitem__, is_last_history[:-1]))

        if is_last_history:
            printer(prefix_without_branch)
            printer(branches[is_last_history[-1]])
        printer("%s\n" % label_lines[0])

        for line in label_lines[1:]:
            printer(''.join(map(stems.__getitem__, is_last_history)))
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
        gdb.Command.__init__(self, 'redpanda heapprof', gdb.COMMAND_USER,
                             gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        parser = argparse.ArgumentParser(description="redpanda heapprof")
        parser.add_argument(
            "-G",
            "--inverted",
            action="store_true",
            help="Compute caller-first profile instead of callee-first")
        parser.add_argument(
            "-a",
            "--addresses",
            action="store_true",
            help="Show raw addresses before resolved symbol names")
        parser.add_argument("--no-symbols",
                            action="store_true",
                            help="Show only raw addresses")
        parser.add_argument(
            "--flame",
            action="store_true",
            help=
            "Write flamegraph data to heapprof.stacks instead of showing the profile"
        )
        parser.add_argument(
            "--min",
            action="store",
            type=int,
            default=0,
            help="Drop branches allocating less than given amount")
        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        root = ProfNode(None)
        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        site = cpu_mem['alloc_site_list_head']

        shared_objects = std_vector(
            gdb.parse_and_eval('\'seastar::shared_objects\''))

        while site:
            size = int(site['size'])
            count = int(site['count'])
            if size:
                n = root
                n.size += size
                n.count += count
                bt = site['backtrace']
                addresses = list(
                    int(f['addr'])
                    for f in seastar_static_vector(bt['_frames']))
                addresses.pop(0)  # drop memory::get_backtrace()
                if args.inverted:
                    seq = reversed(addresses)
                else:
                    seq = addresses
                for addr in seq:
                    n = n.get_or_add(addr)
                    n.size += size
                    n.count += count
            site = site['next']

        def resolve_relative(addr):
            for so in shared_objects:
                sym = resolve(addr + int(so['begin']))
                if sym:
                    return sym
            return None

        def resolver(addr):
            if args.no_symbols:
                return '0x%x' % addr
            if args.addresses:
                return '0x%x %s' % (addr, resolve_relative(addr) or '')
            return resolve_relative(addr) or ('0x%x' % addr)

        if args.flame:
            file_name = 'heapprof.stacks'
            with open(file_name, 'w') as out:
                trace = list()

                def print_node(n):
                    if n.key:
                        trace.append(n.key)
                        trace.extend(n.tail)
                    for c in n.children:
                        print_node(c)
                    if not n.has_children():
                        out.write("%s %d\n" % (';'.join(
                            map(lambda x: '%s' %
                                (x), map(resolver, trace))), n.size))
                    if n.key:
                        del trace[-1 - len(n.tail):]

                print_node(root)
            gdb.write('Wrote %s\n' % (file_name))
        else:

            def node_formatter(n):
                if n.key is None:
                    name = "All"
                else:
                    name = resolver(n.key)
                return "%s (%d, #%d)\n%s" % (name, n.size, n.count, '\n'.join(
                    map(resolver, n.tail)))

            def node_filter(n):
                return n.size >= args.min

            collapse_similar(root)
            print_tree(root,
                       formatter=node_formatter,
                       order_by=lambda n: -n.size,
                       node_filter=node_filter,
                       printer=gdb.write)


redpanda()
redpanda_memory()
redpanda_storage()
redpanda_batch_cache()
redpanda_task_queues()
redpanda_smp_queues()
redpanda_small_objects()
redpanda_task_histogram()
redpanda_heapprof()
