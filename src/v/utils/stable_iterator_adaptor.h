/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/seastarx.h"
#include "utils/exceptions.h"

#include <seastar/util/noncopyable_function.hh>

#include <boost/iterator/iterator_adaptor.hpp>
#include <fmt/format.h>

#include <version>

class iterator_stability_violation final
  : public concurrent_modification_error {
public:
    explicit iterator_stability_violation(ss::sstring why)
      : concurrent_modification_error(std::move(why)) {}
};

/*
 * An iterator implementation that guards against invalidations across
 * scheduling points. It takes a function that tracks the revision of the
 * underlying container. When the iterator is created, it saves the initial
 * revision (stable_revision) and the expecation is that the revision remains
 * same for the liftime of this iterator. This is validated on all iterator
 * operations. The revision func should return a new revision anytime there
 * is a change to the container that can cause invalidations. See topic_table
 * example.
 */

template<class BaseIt, class RevisionType>
class stable_iterator
  : public boost::
      iterator_adaptor<stable_iterator<BaseIt, RevisionType>, BaseIt> {
private:
    using type = stable_iterator<BaseIt, RevisionType>;
    using base = boost::iterator_adaptor<type, BaseIt>;
#ifdef _LIBCPP_VERSION
    using stability_func = ss::noncopyable_function<RevisionType()>;
#else
    // Some iterator algorithms in libstdc++ requires an iterator
    // to be copy-constructible.
    using stability_func = std::function<RevisionType()>;
#endif

public:
    explicit stable_iterator(stability_func&& func, BaseIt base)
      : type::iterator_adaptor_(base)
      , _stability_func(std::move(func))
      , _stable_revision(_stability_func()) {}

    void check() const { validate_revision(); }

private:
    void validate_revision() const {
        auto current_revision = _stability_func();
        if (_stable_revision != current_revision) {
            throw iterator_stability_violation(fmt::format(
              "Iterator stability violated, stable_revision: {}, current "
              "revision: {}",
              _stable_revision,
              current_revision));
        }
    }

    void advance(base::difference_type diff) {
        validate_revision();
        std::advance(this->base_reference(), diff);
    }

    void increment() {
        validate_revision();
        this->base_reference()++;
    }

    base::reference dereference() const {
        validate_revision();
        return *(this->base_reference());
    }

    friend class boost::iterator_core_access;
    stability_func _stability_func;
    RevisionType _stable_revision;
};
