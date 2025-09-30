/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/handlers/alter_user_scram_credentials.h"

#include "cluster/security_frontend.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/types.h"
#include "kafka/server/errors.h"
#include "kafka/server/handlers/details/security.h"
#include "security/acl.h"
#include "security/credential_store.h"
#include "security/scram_algorithm.h"
#include "security/scram_credential.h"

#include <boost/range/iterator_range_core.hpp>

namespace kafka {
namespace {

template<typename T>
kafka::alter_user_scram_credentials_result
generate_error(const T& item, kafka::error_code code, std::string_view msg) {
    return {
      .user = item.name, .error_code = code, .error_message = ss::sstring{msg}};
}

template<std::ranges::input_range Range, typename ErrIter>
void populate_results_with_error(
  const Range& range,
  ErrIter out_it,
  kafka::error_code ec,
  std::string_view msg) {
    std::ranges::transform(range, out_it, [ec, &msg](const auto& item) {
        return generate_error(item, ec, msg);
    });
}

void populate_results_with_error(
  const alter_user_scram_credentials_request_data& req,
  alter_user_scram_credentials_response_data& res,
  kafka::error_code ec,
  std::string_view err_msg) {
    res.results.reserve(req.upsertions.size() + req.deletions.size());
    populate_results_with_error(
      req.upsertions, std::back_inserter(res.results), ec, err_msg);
    populate_results_with_error(
      req.deletions, std::back_inserter(res.results), ec, err_msg);
}

template<typename Iter, typename ErrIter>
Iter validate_range_no_empty_users(Iter begin, Iter end, ErrIter out_it) {
    using type = Iter::value_type;
    auto valid_range_end = std::partition(
      begin, end, [](const type& item) { return !item.name().empty(); });
    std::transform(valid_range_end, end, out_it, [](const type& item) {
        return generate_error(
          item,
          kafka::error_code::unacceptable_credential,
          "Username must not be empty");
    });

    return valid_range_end;
}

template<typename Iter, typename ErrIter>
Iter validate_range_valid_scram_user_name(
  Iter begin, Iter end, ErrIter out_it) {
    using type = Iter::value_type;
    auto valid_range_end = std::partition(begin, end, [](const type& item) {
        return security::validate_scram_username(item.name());
    });
    std::transform(valid_range_end, end, out_it, [](const type& item) {
        return generate_error(
          item,
          kafka::error_code::unacceptable_credential,
          "Invalid SCRAM username");
    });

    return valid_range_end;
}

template<typename Iter, typename ErrIter>
Iter validate_range_valid_scram_mech(Iter begin, Iter end, ErrIter out_it) {
    using type = Iter::value_type;
    auto valid_range_end = std::partition(begin, end, [](const type& item) {
        return kafka::details::kafka_to_security_mechanism(item.mechanism)
          .has_value();
    });

    std::transform(valid_range_end, end, out_it, [](const type& item) {
        return generate_error(
          item,
          kafka::error_code::unsupported_sasl_mechanism,
          "Unknown SCRAM mechanism");
    });

    return valid_range_end;
}

template<typename Iter, typename ErrIter>
Iter validate_range_valid_iterations(
  Iter begin, Iter end, ErrIter out_it, int32_t max_iterations) {
    using type = Iter::value_type;
    const auto min_iterations = [](security::scram_algorithm_t scram_algo) {
        switch (scram_algo) {
        case security::scram_algorithm_t::sha256:
            return security::scram_sha256::min_iterations;
        case security::scram_algorithm_t::sha512:
            return security::scram_sha512::min_iterations;
        }
    };
    auto valid_range_end = std::partition(
      begin, end, [max_iterations, &min_iterations](const type& item) {
          return item.iterations >= min_iterations(
                   kafka::details::kafka_to_security_mechanism(item.mechanism)
                     .value())
                 && item.iterations <= max_iterations;
      });
    std::transform(
      valid_range_end, end, out_it, [max_iterations](const type& item) {
          return generate_error(
            item,
            kafka::error_code::unacceptable_credential,
            item.iterations > max_iterations ? "Too many iterations"
                                             : "Too few iterations");
      });

    return valid_range_end;
}

template<typename Iter, typename ErrIter>
Iter validate_range_superusers_scram_users(
  Iter begin, Iter end, ErrIter out_it) {
    using type = Iter::value_type;
    const auto can_be_modified = [](const type& item) {
        return !std::ranges::contains(
          config::shard_local_cfg().superusers(), item.name());
    };
    auto valid_range_end = std::partition(begin, end, can_be_modified);

    std::transform(valid_range_end, end, out_it, [](const type& item) {
        return generate_error(
          item,
          kafka::error_code::cluster_authorization_failed,
          "Cannot modify superuser SCRAM user");
    });

    return valid_range_end;
}

template<typename UpsertIter, typename DeleteIter, typename ErrIter>
std::pair<UpsertIter, DeleteIter> validate_range_no_duplicates(
  UpsertIter upsert_begin,
  UpsertIter upsert_end,
  DeleteIter delete_begin,
  DeleteIter delete_end,
  ErrIter out_it) {
    chunked_hash_map<std::string_view, uint32_t> freq;
    freq.reserve(
      std::distance(upsert_begin, upsert_end)
      + std::distance(delete_begin, delete_end));
    const auto count = [&freq]<typename Iter>(Iter begin, Iter end) {
        for (const auto& r : boost::make_iterator_range(begin, end)) {
            freq[r.name()]++;
        }
    };
    count(upsert_begin, upsert_end);
    count(delete_begin, delete_end);

    const auto partition_on_single_user_instance = [&freq]<typename Iter>(
                                                     Iter begin, Iter end) {
        return std::partition(begin, end, [&freq](const auto& item) {
            return freq[item.name()] == 1;
        });
    };

    auto valid_upsert_range_end = partition_on_single_user_instance(
      upsert_begin, upsert_end);
    auto valid_delete_range_end = partition_on_single_user_instance(
      delete_begin, delete_end);

    // We only want to respone _once_ for every duplicated user, so we will
    // iterate through the partitioned list and generate an error if we
    // encounter a user with a frequency greater than 1.  Once added, reset the
    // frequency to 1 to skip the user if found again
    const auto insert_error = [&freq]<typename Iter, typename OutIter>(
                                Iter begin, Iter end, OutIter out_it) {
        for (auto it = begin; it != end; it++) {
            if (freq[it->name()] > 1) {
                *out_it = generate_error(
                  *it,
                  kafka::error_code::duplicate_resource,
                  "A user credential cannot be altered twice in the same "
                  "request");
                freq[it->name()] = 1;
                ++out_it;
            }
        }
    };

    insert_error(valid_upsert_range_end, upsert_end, out_it);
    insert_error(valid_delete_range_end, delete_end, out_it);

    return {valid_upsert_range_end, valid_delete_range_end};
}

error_code map_security_error_code(std::error_code ec) {
    if (!ec) {
        return error_code::none;
    }

    if (ec.category() == cluster::error_category()) {
        return map_topic_error_code(cluster::errc(ec.value()));
    }

    return error_code::unknown_server_error;
}
} // namespace
template<>
ss::future<response_ptr> alter_user_scram_credentials_handler::handle(
  request_context ctx, ss::smp_service_group) {
    static constexpr auto max_iterations = 16384;
    alter_user_scram_credentials_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    alter_user_scram_credentials_response res;

    if (!ctx.authorized(
          security::acl_operation::alter, security::default_cluster_name)) {
        populate_results_with_error(
          request.data,
          res.data,
          error_code::cluster_authorization_failed,
          error_code_to_str(error_code::cluster_authorization_failed));

        co_return co_await ctx.respond(std::move(res));
    }

    if (!ctx.audit()) {
        res.data.results.reserve(
          request.data.upsertions.size() + request.data.deletions.size());

        populate_results_with_error(
          request.data,
          res.data,
          error_code::broker_not_available,
          "Broker not available - audit system failure");

        co_return co_await ctx.respond(std::move(res));
    }

    const auto no_superuser_access = !ctx.connection()->has_superuser_access();

    auto upsert_begin = request.data.upsertions.begin();
    auto upsert_valid_range_end = validate_range_no_empty_users(
      upsert_begin,
      request.data.upsertions.end(),
      std::back_inserter(res.data.results));

    auto invalid_scram_name_it = validate_range_valid_scram_user_name(
      upsert_begin,
      upsert_valid_range_end,
      std::back_inserter(res.data.results));
    upsert_valid_range_end = invalid_scram_name_it;

    auto invalid_mech_it = validate_range_valid_scram_mech(
      upsert_begin,
      upsert_valid_range_end,
      std::back_inserter(res.data.results));

    upsert_valid_range_end = invalid_mech_it;

    auto invalid_iterations_it = validate_range_valid_iterations(
      upsert_begin,
      upsert_valid_range_end,
      std::back_inserter(res.data.results),
      max_iterations);
    upsert_valid_range_end = invalid_iterations_it;

    if (no_superuser_access) {
        auto superuser_name_it = validate_range_superusers_scram_users(
          upsert_begin,
          upsert_valid_range_end,
          std::back_inserter(res.data.results));
        upsert_valid_range_end = superuser_name_it;
    }

    auto deletions_begin = request.data.deletions.begin();
    auto deletions_valid_range_end = validate_range_no_empty_users(
      deletions_begin,
      request.data.deletions.end(),
      std::back_inserter(res.data.results));

    auto invalid_mech_deletions_it = validate_range_valid_scram_mech(
      deletions_begin,
      deletions_valid_range_end,
      std::back_inserter(res.data.results));
    deletions_valid_range_end = invalid_mech_deletions_it;

    if (no_superuser_access) {
        auto superuser_name_deletions_it
          = validate_range_superusers_scram_users(
            deletions_begin,
            deletions_valid_range_end,
            std::back_inserter(res.data.results));
        deletions_valid_range_end = superuser_name_deletions_it;
    }

    auto [upsert_dup_it, delete_dup_it] = validate_range_no_duplicates(
      upsert_begin,
      upsert_valid_range_end,
      deletions_begin,
      deletions_valid_range_end,
      std::back_inserter(res.data.results));

    upsert_valid_range_end = upsert_dup_it;
    deletions_valid_range_end = delete_dup_it;

    const auto make_creds = [](const scram_credential_upsertion& u) {
        security::acl_principal p{security::principal_type::user, u.name};
        auto mech = details::kafka_to_security_mechanism(u.mechanism).value();
        switch (mech) {
        case security::scram_algorithm_t::sha256:
            return security::scram_sha256::make_credentials(
              std::move(p), u.salted_password, u.salt, u.iterations);
        case security::scram_algorithm_t::sha512:
            return security::scram_sha512::make_credentials(
              std::move(p), u.salted_password, u.salt, u.iterations);
        }
    };

    for (const auto& u :
         boost::make_iterator_range(upsert_begin, upsert_valid_range_end)) {
        security::credential_user user{u.name};
        auto user_exists = ctx.credentials().contains(user);
        auto creds = make_creds(u);

        std::error_code ec;

        if (user_exists) {
            vlog(klog.debug, "Updating SCRAM credentials for user {}", u.name);
            ec = co_await ctx.security_frontend().update_user(
              std::move(user),
              std::move(creds),
              model::timeout_clock::now() + 5s);

        } else {
            vlog(klog.debug, "Creating SCRAM credentials for user {}", u.name);
            ec = co_await ctx.security_frontend().create_user(
              std::move(user),
              std::move(creds),
              model::timeout_clock::now() + 5s);
        }
        vlog(
          klog.debug,
          "Results for updating/creating user {}: ({}:{})",
          u.name,
          ec,
          ec.message());

        res.data.results.emplace_back(
          alter_user_scram_credentials_result{
            .user = u.name, .error_code = map_security_error_code(ec)});
    }

    const auto user_exists_and_same_scram_mech =
      [&ctx](const security::credential_user& u, scram_mechanism m) {
          auto maybe_creds = ctx.credentials().get<security::scram_credential>(
            u);
          if (!maybe_creds.has_value()) {
              return false;
          }
          const auto& creds = maybe_creds.value();
          return details::key_size_to_mechanism(creds.stored_key().size()) == m;
      };

    for (const auto& u : boost::make_iterator_range(
           deletions_begin, deletions_valid_range_end)) {
        security::credential_user user{u.name};

        if (!user_exists_and_same_scram_mech(user, u.mechanism)) {
            res.data.results.emplace_back(
              alter_user_scram_credentials_result{
                .user = u.name,
                .error_code = error_code::resource_not_found,
                .error_message
                = "Attempt to delete a user credential that does not exist"});
        } else {
            vlog(klog.debug, "Deleting SCRAM credentials for user {}", u.name);
            auto ec = co_await ctx.security_frontend().delete_user(
              std::move(user), model::timeout_clock::now() + 5s);
            vlog(
              klog.debug,
              "Results for deleting user {}: ({}:{})",
              u.name,
              ec,
              ec.message());
            res.data.results.emplace_back(
              alter_user_scram_credentials_result{
                .user = u.name, .error_code = map_security_error_code(ec)});
        }
    }

    co_return co_await ctx.respond(std::move(res));
}
} // namespace kafka
