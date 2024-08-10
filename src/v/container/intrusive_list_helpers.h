/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

/*
 * This file contains convenience aliases that make boost::intrusive::list
 * easier to use.
 */

#include <boost/intrusive/list.hpp>

/**
 * An auto-unlink intrusive list hook.
 */
using intrusive_list_hook = boost::intrusive::list_member_hook<
  boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;

/**
 * An intrusive list.
 *
 * An intrusive_list always uses an auto-unlink hook.
 * Beware that intrusive_list::size() is an O(n) operation, since it has to walk
 * the entire list.
 *
 * Example usage:
 *
 *   class Foo {
 *     // Note that the listHook member variable needs to be visible
 *     // to the code that defines the intrusive_list instantiation.
 *     // The list hook can be made public, or you can make the other class a
 *     // friend.
 *     intrusive_list_hook listHook;
 *   };
 *
 *   using FooList = intrusive_list<Foo, &Foo::listHook>;
 *
 *   Foo *foo = new Foo();
 *   FooList myList;
 *   myList.push_back(*foo);
 *
 * Note that each intrusive_list_hook can only be part of a single list at any
 * given time.  If you need the same object to be stored in two lists at once,
 * you need to use two different intrusive_list_hook member variables.
 *
 * The elements stored in the list must contain an intrusive_list_hook member
 * variable.
 */
template<typename T, intrusive_list_hook T::*PtrToMember>
using intrusive_list = boost::intrusive::list<
  T,
  boost::intrusive::member_hook<T, intrusive_list_hook, PtrToMember>,
  boost::intrusive::constant_time_size<false>>;

/**
 * A safe-link intrusive list hook.
 */
using safe_intrusive_list_hook = boost::intrusive::list_member_hook<
  boost::intrusive::link_mode<boost::intrusive::safe_link>>;

/**
 * An intrusive list with const-time size() method.
 *
 * A counted_intrusive_list always uses a safe-link hook.
 * counted_intrusive_list::size() is an O(1) operation. Users of this type
 * of lists need to remove a member from a list by calling one of the
 * methods on the list (e.g., erase(), pop_front(), etc.), rather than
 * calling unlink on the member's list hook. Given references to a
 * list and a member, a constant-time removal operation can be
 * accomplished by list.erase(list.iterator_to(member)). Also, when a
 * member is destroyed, it is NOT automatically removed from the list.
 *
 * Example usage:
 *
 *   class Foo {
 *     // Note that the listHook member variable needs to be visible
 *     // to the code that defines the counted_intrusive_list instantiation.
 *     // The list hook can be made public, or you can make the other class a
 *     // friend.
 *     safe_intrusive_link_hook listHook;
 *   };
 *
 *   using FooList = counted_intrusive_list<Foo, &Foo::listHook> FooList;
 *
 *   Foo *foo = new Foo();
 *   FooList myList;
 *   myList.push_back(*foo);
 *   myList.pop_front();
 *
 * Note that each safe_intrusive_link_hook can only be part of a single list at
 * any given time.  If you need the same object to be stored in two lists at
 * once, you need to use two different safe_intrusive_link_hook member
 * variables.
 *
 * The elements stored in the list must contain an safe_intrusive_link_hook
 * member variable.
 */
template<typename T, safe_intrusive_list_hook T::*PtrToMember>
using counted_intrusive_list = boost::intrusive::list<
  T,
  boost::intrusive::member_hook<T, safe_intrusive_list_hook, PtrToMember>,
  boost::intrusive::constant_time_size<true>>;

/**
 * A safe intrusive list with non-const-time size() method.
 *
 * Use this when you want a safe mode list, but don't want to pay the size cost
 * of the extra field to track the size.
 */
template<typename T, safe_intrusive_list_hook T::*PtrToMember>
using uncounted_intrusive_list = boost::intrusive::list<
  T,
  boost::intrusive::member_hook<T, safe_intrusive_list_hook, PtrToMember>,
  boost::intrusive::constant_time_size<false>>;
